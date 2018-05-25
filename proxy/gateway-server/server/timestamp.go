package server

const physicalShiftBits = 18

// ComposeTS creates a ts from physical and logical parts.
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

// ExtractPhysical returns a ts's physical part.
func ExtractPhysical(ts uint64) int64 {
	return int64(ts >> physicalShiftBits)
}
