package blob_store

import (
	"fmt"
	"path/filepath"
	"master-server/engine/errors"
	"util/log"
)

func CheckKey(path string, keysMap map[interface{}]uint8) error {
	dirList, err := GetFiles(path)
	if err != nil {
		return err
	}

	var hit uint32
	for _, dir := range dirList {
		fileName := fmt.Sprintf("%s%s%s", path, string(filepath.Separator), dir)
		reader, err := NewReader(fileName)
		if err != nil {
			return err
		}
		err = reader.ReadHeaderMock()
		if err != nil {
			return errors.New(fmt.Sprintf("read header for blob-file err: %v", err))
		}
		records := reader.ReadRecords()
		for _, r := range records {
			if _, ok := keysMap[r.key]; ok {
				log.Warn("still exist key : %v ", string(r.key))
				hit++
			}
		}
	}

	if hit != 0 {
		return errors.New(fmt.Sprintf("check key number: %v, still exist key number: %v", len(keysMap), hit))
	}
	return nil
}
