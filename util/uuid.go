package util

import (
	"sync"
	"sync/atomic"
	"time"
	"net"
	"errors"
	"encoding/base64"
)

var (
	lastTimestamp uint64
	sequenceNum   uint32
	lock          sync.Mutex
	localMacAddrs *[6]byte
)

func init() {
	addr, err := macAddr()
	if err != nil {
		panic("this host isn't suitable to be used as router, it can't generate doc id")
	}
	localMacAddrs = addr
}

// uuid = mac address(6byte) + timestamp(6byte) + sequence number(4byte)
func NewUuid() string  {
	var uuidBytes =  NewUuidBytes()
	return base64.URLEncoding.EncodeToString(uuidBytes[:])
}


func NewUuidBytes() [16]byte {
	curSeqNum := atomic.AddUint32(&sequenceNum, 1)
	curTimestamp := uint64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())

	lock.Lock()
	if lastTimestamp > curTimestamp {
		curTimestamp = lastTimestamp
	}

	if curSeqNum == 0 {
		curTimestamp++
	}

	lastTimestamp = curTimestamp
	lock.Unlock()

	var autoIdBytes [16]byte
	for i := 0; i < 6; i++ {
		autoIdBytes[i] = (*localMacAddrs)[i]
	}
	for i := 0; i < 6; i++ {
		autoIdBytes[11-i] = byte(curTimestamp >> uint(i*8))
	}
	for i := 0; i < 4; i++ {
		autoIdBytes[15-i] = byte(curSeqNum >> uint(i*8))
	}

	return autoIdBytes
}

func macAddr() (*[6]byte, error) {
	var macAddrss [6]byte
	intfs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, intf := range intfs {
		if intf.Flags&net.FlagUp == 0 {
			continue
		}
		if intf.Flags&(net.FlagLoopback|net.FlagPointToPoint) != 0 {
			continue
		}

		for i := 0; i < 6; i++ {
			macAddrss[i] = intf.HardwareAddr[i]
		}
		return &macAddrss, err
	}

	return nil, errors.New("mac addr not found")
}