package inet

import (
	"strings"
	"net"
	"strconv"
)

func N32toa(ipnr int32) net.IP {
	var bytes [4]byte
	bytes[0] = byte(ipnr & 0xFF)
	bytes[1] = byte((ipnr >> 8) & 0xFF)
	bytes[2] = byte((ipnr >> 16) & 0xFF)
	bytes[3] = byte((ipnr >> 24) & 0xFF)

	return net.IPv4(bytes[3],bytes[2],bytes[1],bytes[0])
}

func Aton32(ipnr string) int32 {
	bits := strings.Split(ipnr, ".")

	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])

	var sum int32

	sum += int32(b0) << 24
	sum += int32(b1) << 16
	sum += int32(b2) << 8
	sum += int32(b3)

	return sum
}

func GenAddrId(ip string, port uint64) uint64 {
	id := uint64(Aton32(ip))
	id <<= 32
	id += port
	return id
}
