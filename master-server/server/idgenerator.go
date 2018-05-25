package server

import (
	"sync"

	"util/log"
	sErr "master-server/engine/errors"
)

var genStep uint64 = 10

type IDGenerator interface {
	GenID() (uint64, error)
}

type ClusterIDGenerator struct {
	*idGenerator
}

func NewClusterIDGenerator(store Store) IDGenerator {
	return &ClusterIDGenerator{idGenerator: NewIDGenerator([]byte(AUTO_INCREMENT_ID), genStep, store)}
}

type idGenerator struct {
	lock   sync.Mutex
	base uint64
	end  uint64

	key []byte
	step uint64

	store Store
}

func NewIDGenerator(key []byte, step uint64, store Store) *idGenerator {
	return &idGenerator{key: key, step: step, store: store}
}

func (id *idGenerator) GenID() (uint64, error) {
	id.lock.Lock()
	defer id.lock.Unlock()

	if id.base == id.end {
		log.Debug("[GENID] before generate!!!!!! (base %d, end %d)", id.base, id.end)
		end, err := id.generate()
		if err != nil {
			return 0, err
		}

		id.end = end
		id.base = id.end - id.step
		log.Debug("[GENID] after generate!!!!!! (base %d, end %d)", id.base, id.end)
	}

	id.base++

	return id.base, nil
}

func (id *idGenerator) get(key []byte) ([]byte, error) {
	value, err := id.store.Get(key)
	if err != nil {
		if err == sErr.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return value, nil
}

func (id *idGenerator) put(key, value []byte) error {
	return id.store.Put(key, value)
}

func (id *idGenerator) generate() (uint64, error) {
	value, err := id.get(id.key)
	if err != nil {
		return 0, err
	}

	var end uint64

	if value != nil {
		end, err = bytesToUint64(value)
		if err != nil {
			return 0, err
		}
	}
	end += id.step
	value = uint64ToBytes(end)
	err = id.put(id.key, value)
	if err != nil {
		return 0, err
	}

	return end, nil
}
