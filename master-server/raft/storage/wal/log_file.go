package wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"

	"master-server/raft/proto"
	"util/log"
)

type logEntryFile struct {
	dir  string
	name logFileName

	f     *os.File
	r     recordReadAt
	w     *recordWriter
	index logEntryIndex
}

func openLogEntryFile(dir string, name logFileName, isLastOne bool) (*logEntryFile, error) {
	p := path.Join(dir, name.String())
	f, err := os.OpenFile(p, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	lf := &logEntryFile{
		dir:  dir,
		name: name,
		f:    f,
		r:    newRecordReader(f),
	}

	if !isLastOne {
		// 读取索引数据
		if err = lf.ReadIndex(); err != nil {
			return nil, err
		}
	} else {
		// 重建索引
		toffset, err := lf.ReBuildIndex()
		if err != nil {
			return nil, err
		}
		// 打开写
		if err = lf.OpenWrite(); err != nil {
			return nil, err
		}
		if toffset > 0 {
			// 截断索引及后面的数据
			if err := lf.w.Truncate(toffset); err != nil {
				return nil, err
			}
		}
	}

	return lf, nil
}

func createLogEntryFile(dir string, name logFileName) (*logEntryFile, error) {
	p := path.Join(dir, name.String())
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	lf := &logEntryFile{
		dir:  dir,
		name: name,
		f:    f,
		r:    newRecordReader(f),
	}

	if err := lf.OpenWrite(); err != nil {
		return nil, err
	}

	return lf, nil
}

func (lf *logEntryFile) ReadIndex() error {
	info, err := lf.f.Stat()
	if err != nil {
		return err
	}

	// read footer
	var footer footerRecord
	if info.Size() < int64(footer.Size()) {
		return NewCorruptError(lf.f.Name(), 0, "too small footer")
	}
	offset := info.Size() - int64(recordSize(footer))
	rec, err := lf.r.ReadAt(offset)
	if err != nil {
		return err
	}
	if rec.recType != recTypeFooter {
		return NewCorruptError(lf.f.Name(), offset, "wrong footer record type")
	}
	if rec.dataLen != footer.Size() {
		return NewCorruptError(lf.f.Name(), offset, "wrong footer size")
	}
	footer.Decode(rec.data)
	if !bytes.Equal(footer.magic, footerMagic) {
		return NewCorruptError(lf.f.Name(), offset, "wrong footer magic")
	}

	// read index data
	offset = int64(footer.indexOffset)
	rec, err = lf.r.ReadAt(offset)
	if err != nil {
		return err
	}
	if rec.recType != recTypeIndex {
		return NewCorruptError(lf.f.Name(), offset, "wrong index record type")
	}
	lf.index = decodeLogIndex(rec.data)

	return nil
}

func (lf *logEntryFile) ReBuildIndex() (truncateOffset int64, err error) {
	lf.index = nil

	// 获取文件大小
	info, err := lf.f.Stat()
	if err != nil {
		return 0, err
	}
	filesize := info.Size()

	var rec record
	var offset int64
	r := newRecordReader(lf.f)
	for {
		offset, rec, err = r.Read()
		if err != nil {
			if err == io.EOF || err == errUnexpectedEOF {
				if offset < filesize {
					log.Warn("truncate last logfile's N@%d index at: %d, filesize: %d", lf.name.seq, offset, filesize)
				}
				return offset, nil // 截断，返回没有错误
			}
			return 0, err
		}

		// log entry 更新索引
		if rec.recType == recTypeLogEntry {
			ent := &proto.Entry{}
			ent.Decode(rec.data)
			lf.index = lf.index.Append(uint32(offset), ent)
		} else if rec.recType == recTypeIndex { // 直接截断索引，即使索引写完全了再写一遍，防止索引或footer等没写完整
			return offset, nil
		} else {
			return 0, NewCorruptError(lf.f.Name(), offset, fmt.Sprintf("wrong log entry record type: %s", rec.recType.String()))
		}
	}
}

func (lf *logEntryFile) Name() logFileName {
	return lf.name
}

func (lf *logEntryFile) Seq() uint64 {
	return lf.name.seq
}

func (lf *logEntryFile) Len() int {
	return lf.index.Len()
}

func (lf *logEntryFile) FirstIndex() uint64 {
	return lf.index.First()
}

func (lf *logEntryFile) LastIndex() uint64 {
	return lf.index.Last()
}

// Get get log entry
func (lf *logEntryFile) Get(i uint64) (*proto.Entry, error) {
	item, err := lf.index.Get(i)
	if err != nil {
		return nil, err
	}

	rec, err := lf.r.ReadAt(int64(item.offset))
	if err != nil {
		return nil, err
	}

	ent := &proto.Entry{}
	ent.Decode(rec.data)

	return ent, nil
}

// Term get log's term
func (lf *logEntryFile) Term(i uint64) (uint64, error) {
	item, err := lf.index.Get(i)
	if err != nil {
		return 0, err
	}
	return item.logterm, nil
}

// Truncate 截断最近的日志
func (lf *logEntryFile) Truncate(index uint64) error {
	if lf.Len() == 0 {
		return nil
	}

	item, err := lf.index.Get(index)
	if err != nil {
		return err
	}

	// 截断文件
	offset := int64(item.offset)
	if err = lf.w.Truncate(offset); err != nil {
		return err
	}

	// 截断索引
	lf.index, err = lf.index.Truncate(index)
	return err
}

func (lf *logEntryFile) Save(ent *proto.Entry) error {
	// 写入文件
	offset := lf.w.Offset()
	if err := lf.w.Write(recTypeLogEntry, ent); err != nil {
		return err
	}

	// 更新索引
	lf.index = lf.index.Append(uint32(offset), ent)

	return nil
}

func (lf *logEntryFile) OpenWrite() error {
	if lf.w != nil {
		return nil
	}

	lf.w = newRecordWriter(lf.f)
	return nil
}

func (lf *logEntryFile) WriteOffset() int64 {
	return lf.w.Offset()
}

func (lf *logEntryFile) Flush() error {
	return lf.w.Flush()
}

// Sync flush write buffer and sync to disk
func (lf *logEntryFile) Sync() error {
	return lf.w.Sync()
}

func (lf *logEntryFile) FinishWrite() error {
	var err error

	// write log index data
	recOffset := lf.w.Offset()
	if err = lf.w.Write(recTypeIndex, lf.index); err != nil {
		return err
	}

	// write log file footer
	footer := &footerRecord{
		indexOffset: uint64(recOffset),
	}
	if err = lf.w.Write(recTypeFooter, footer); err != nil {
		return err
	}

	if err := lf.w.Close(); err != nil {
		return err
	}
	lf.w = nil
	return nil
}

// Close 关闭读写，关闭文件
func (lf *logEntryFile) Close() error {
	if lf.w != nil {
		if err := lf.w.Close(); err != nil {
			return err
		}
		lf.w = nil
	}

	return lf.f.Close()
}
