package util

import (
	"encoding/binary"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"util/bufalloc"
)

const (
	msgHeaderSize        = 16
	msgVersion    uint16 = 1
	msgMagic      uint16 = 0xdaf4
)

// The RPC format is header + protocol buffer body
// Header is 16 bytes, format:
//  | 0xdaf4(2 bytes magic value) | 0x01(version 2 bytes) | msg_len(4 bytes) | msg_id(8 bytes) |,
// all use bigendian.

// WriteMessage writes a protocol buffer message to writer.
func WriteMessage(w io.Writer, msgID uint64, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}

	//var header [msgHeaderSize]byte
	buffer := bufalloc.AllocBuffer(msgHeaderSize)
	defer bufalloc.FreeBuffer(buffer)
	header := buffer.Alloc(msgHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], msgMagic)
	binary.BigEndian.PutUint16(header[2:4], msgVersion)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	binary.BigEndian.PutUint64(header[8:16], msgID)

	if _, err = w.Write(header[:]); err != nil {
		return errors.Trace(err)
	}

	_, err = w.Write(body)
	return errors.Trace(err)
}

// ReadMessage reads a protocol buffer message from reader.
func ReadMessage(r io.Reader, msg proto.Message) (uint64, error) {
	//var header [msgHeaderSize]byte
	buffer1 := bufalloc.AllocBuffer(msgHeaderSize)
	defer bufalloc.FreeBuffer(buffer1)
	header := buffer1.Alloc(msgHeaderSize)
	_, err := io.ReadFull(r, header[:])
	if err != nil {
		return 0, errors.Trace(err)
	}

	if magic := binary.BigEndian.Uint16(header[0:2]); magic != msgMagic {
		return 0, errors.Errorf("mismatch header magic %x != %x", magic, msgMagic)
	}

	// skip version now.

	msgLen := binary.BigEndian.Uint32(header[4:8])
	msgID := binary.BigEndian.Uint64(header[8:])

	//body := make([]byte, msgLen)
	buffer2 := bufalloc.AllocBuffer(int(msgLen))
	defer bufalloc.FreeBuffer(buffer2)
	body := buffer2.Alloc(int(msgLen))
	_, err = io.ReadFull(r, body)
	if err != nil {
		return 0, errors.Trace(err)
	}

	err = proto.Unmarshal(body, msg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return msgID, nil
}

func ReadMessageBytes(r io.Reader, msg proto.Message) (uint64, int, error) {
	//var header [msgHeaderSize]byte
	buffer1 := bufalloc.AllocBuffer(msgHeaderSize)
	defer bufalloc.FreeBuffer(buffer1)
	header := buffer1.Alloc(msgHeaderSize)
	var nread int
	n, err := io.ReadFull(r, header[:])
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	nread += n

	if magic := binary.BigEndian.Uint16(header[0:2]); magic != msgMagic {
		return 0, 0, errors.Errorf("mismatch header magic %x != %x", magic, msgMagic)
	}

	// skip version now.

	msgLen := binary.BigEndian.Uint32(header[4:8])
	msgID := binary.BigEndian.Uint64(header[8:])

	//body := make([]byte, msgLen)
	buffer2 := bufalloc.AllocBuffer(int(msgLen))
	defer bufalloc.FreeBuffer(buffer2)
	body := buffer2.Alloc(int(msgLen))
	n, err = io.ReadFull(r, body)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	nread += n

	err = proto.Unmarshal(body, msg)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return msgID, nread, nil
}

func WriteMessageBytes(w io.Writer, msgID uint64, msg proto.Message) (int, error) {
	var nwritten int
	body, err := proto.Marshal(msg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	//var header [msgHeaderSize]byte
	buffer := bufalloc.AllocBuffer(msgHeaderSize)
	defer bufalloc.FreeBuffer(buffer)
	header := buffer.Alloc(msgHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], msgMagic)
	binary.BigEndian.PutUint16(header[2:4], msgVersion)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	binary.BigEndian.PutUint64(header[8:16], msgID)

	n, err := w.Write(header[:])
	if err != nil {
		return 0, errors.Trace(err)
	}
	nwritten += n

	n, err = w.Write(body)
	nwritten += n
	return nwritten, errors.Trace(err)
}
