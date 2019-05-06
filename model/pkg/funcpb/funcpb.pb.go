// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: funcpb.proto

/*
	Package funcpb is a generated protocol buffer package.

	It is generated from these files:
		funcpb.proto

	It has these top-level messages:
*/
package funcpb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "github.com/gogo/protobuf/gogoproto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type FunctionID int32

const (
	FunctionID_kFuncHeartbeat           FunctionID = 0
	FunctionID_kFuncRawGet              FunctionID = 1
	FunctionID_kFuncRawPut              FunctionID = 2
	FunctionID_kFuncRawDelete           FunctionID = 3
	FunctionID_kFuncRawExecute          FunctionID = 4
	FunctionID_kFuncSelect              FunctionID = 10
	FunctionID_kFuncInsert              FunctionID = 11
	FunctionID_kFuncDelete              FunctionID = 12
	FunctionID_kFuncUpdate              FunctionID = 13
	FunctionID_KFuncReplace             FunctionID = 14
	FunctionID_kFuncWatchGet            FunctionID = 50
	FunctionID_kFuncPureGet             FunctionID = 51
	FunctionID_kFuncWatchPut            FunctionID = 52
	FunctionID_kFuncWatchDel            FunctionID = 53
	FunctionID_kFuncLock                FunctionID = 200
	FunctionID_kFuncLockUpdate          FunctionID = 201
	FunctionID_kFuncUnlock              FunctionID = 202
	FunctionID_kFuncUnlockForce         FunctionID = 203
	FunctionID_kFuncLockWatch           FunctionID = 204
	FunctionID_kFuncLockGet             FunctionID = 205
	FunctionID_kFuncTxnPrepare          FunctionID = 301
	FunctionID_kFuncTxnDecide           FunctionID = 302
	FunctionID_kFuncTxnClearup          FunctionID = 303
	FunctionID_kFuncTxnGetLockInfo      FunctionID = 304
	FunctionID_kFuncTxnSelect           FunctionID = 305
	FunctionID_kFuncTxnScan             FunctionID = 306
	FunctionID_kFuncCreateRange         FunctionID = 1001
	FunctionID_kFuncDeleteRange         FunctionID = 1002
	FunctionID_kFuncRangeTransferLeader FunctionID = 1003
	FunctionID_kFuncUpdateRange         FunctionID = 1004
	FunctionID_kFuncGetPeerInfo         FunctionID = 1005
	FunctionID_kFuncSetNodeLogLevel     FunctionID = 1006
	FunctionID_kFuncOfflineRange        FunctionID = 1007
	FunctionID_kFuncReplaceRange        FunctionID = 1008
	FunctionID_kFuncAdmin               FunctionID = 2001
)

var FunctionID_name = map[int32]string{
	0:    "kFuncHeartbeat",
	1:    "kFuncRawGet",
	2:    "kFuncRawPut",
	3:    "kFuncRawDelete",
	4:    "kFuncRawExecute",
	10:   "kFuncSelect",
	11:   "kFuncInsert",
	12:   "kFuncDelete",
	13:   "kFuncUpdate",
	14:   "KFuncReplace",
	50:   "kFuncWatchGet",
	51:   "kFuncPureGet",
	52:   "kFuncWatchPut",
	53:   "kFuncWatchDel",
	200:  "kFuncLock",
	201:  "kFuncLockUpdate",
	202:  "kFuncUnlock",
	203:  "kFuncUnlockForce",
	204:  "kFuncLockWatch",
	205:  "kFuncLockGet",
	301:  "kFuncTxnPrepare",
	302:  "kFuncTxnDecide",
	303:  "kFuncTxnClearup",
	304:  "kFuncTxnGetLockInfo",
	305:  "kFuncTxnSelect",
	306:  "kFuncTxnScan",
	1001: "kFuncCreateRange",
	1002: "kFuncDeleteRange",
	1003: "kFuncRangeTransferLeader",
	1004: "kFuncUpdateRange",
	1005: "kFuncGetPeerInfo",
	1006: "kFuncSetNodeLogLevel",
	1007: "kFuncOfflineRange",
	1008: "kFuncReplaceRange",
	2001: "kFuncAdmin",
}
var FunctionID_value = map[string]int32{
	"kFuncHeartbeat":           0,
	"kFuncRawGet":              1,
	"kFuncRawPut":              2,
	"kFuncRawDelete":           3,
	"kFuncRawExecute":          4,
	"kFuncSelect":              10,
	"kFuncInsert":              11,
	"kFuncDelete":              12,
	"kFuncUpdate":              13,
	"KFuncReplace":             14,
	"kFuncWatchGet":            50,
	"kFuncPureGet":             51,
	"kFuncWatchPut":            52,
	"kFuncWatchDel":            53,
	"kFuncLock":                200,
	"kFuncLockUpdate":          201,
	"kFuncUnlock":              202,
	"kFuncUnlockForce":         203,
	"kFuncLockWatch":           204,
	"kFuncLockGet":             205,
	"kFuncTxnPrepare":          301,
	"kFuncTxnDecide":           302,
	"kFuncTxnClearup":          303,
	"kFuncTxnGetLockInfo":      304,
	"kFuncTxnSelect":           305,
	"kFuncTxnScan":             306,
	"kFuncCreateRange":         1001,
	"kFuncDeleteRange":         1002,
	"kFuncRangeTransferLeader": 1003,
	"kFuncUpdateRange":         1004,
	"kFuncGetPeerInfo":         1005,
	"kFuncSetNodeLogLevel":     1006,
	"kFuncOfflineRange":        1007,
	"kFuncReplaceRange":        1008,
	"kFuncAdmin":               2001,
}

func (x FunctionID) String() string {
	return proto.EnumName(FunctionID_name, int32(x))
}
func (FunctionID) EnumDescriptor() ([]byte, []int) { return fileDescriptorFuncpb, []int{0} }

func init() {
	proto.RegisterEnum("funcpb.FunctionID", FunctionID_name, FunctionID_value)
}

func init() { proto.RegisterFile("funcpb.proto", fileDescriptorFuncpb) }

var fileDescriptorFuncpb = []byte{
	// 477 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x92, 0x5d, 0x8e, 0xd3, 0x30,
	0x10, 0xc7, 0x37, 0x01, 0x36, 0xc2, 0xdb, 0x6d, 0xa7, 0xd3, 0x82, 0x16, 0x24, 0x7a, 0x00, 0x1e,
	0x40, 0x62, 0xe1, 0x00, 0xb0, 0x65, 0x4b, 0x45, 0x05, 0x55, 0x29, 0xe2, 0xd9, 0x75, 0x26, 0xa1,
	0xda, 0x60, 0x47, 0x5e, 0x87, 0xed, 0x51, 0xb8, 0x04, 0x9f, 0xa7, 0x28, 0x5f, 0x12, 0x1c, 0x00,
	0x09, 0x95, 0x27, 0xbe, 0x39, 0x02, 0xb2, 0xe3, 0xa6, 0xed, 0x5b, 0xe6, 0x97, 0xf9, 0xff, 0xfd,
	0xf7, 0x78, 0x58, 0x2d, 0x29, 0xa4, 0xc8, 0x27, 0x57, 0x72, 0xad, 0x8c, 0xc2, 0xed, 0xb2, 0xba,
	0xd8, 0x4e, 0x55, 0xaa, 0x1c, 0xba, 0x6a, 0xbf, 0xca, 0xbf, 0x97, 0xbf, 0x9c, 0x61, 0xec, 0xb0,
	0x90, 0xc2, 0x4c, 0x95, 0xec, 0x77, 0x11, 0x59, 0xfd, 0xc8, 0x96, 0x77, 0x88, 0x6b, 0x33, 0x21,
	0x6e, 0x60, 0x0b, 0x1b, 0x6c, 0xc7, 0xb1, 0x11, 0x3f, 0xe9, 0x91, 0x81, 0x60, 0x1d, 0x0c, 0x0b,
	0x03, 0x61, 0xa5, 0x1a, 0xf1, 0x93, 0x2e, 0x65, 0x64, 0x08, 0x4e, 0x61, 0x8b, 0x35, 0x96, 0xec,
	0xf6, 0x8c, 0x44, 0x61, 0x08, 0x4e, 0x57, 0xca, 0x07, 0x94, 0x91, 0x30, 0xc0, 0x2a, 0xd0, 0x97,
	0xc7, 0xa4, 0x0d, 0xec, 0x54, 0xc0, 0xfb, 0xd4, 0x2a, 0xf0, 0x30, 0x8f, 0xb9, 0x21, 0xd8, 0x45,
	0x60, 0xb5, 0xbb, 0xce, 0x98, 0xf2, 0x8c, 0x0b, 0x82, 0x3a, 0x36, 0xd9, 0xae, 0x6b, 0x79, 0xc4,
	0x8d, 0x78, 0x6c, 0x23, 0x5e, 0xb3, 0x4d, 0x0e, 0x0d, 0x0b, 0x4d, 0x96, 0xec, 0x6f, 0x36, 0xd9,
	0xd8, 0xd7, 0x37, 0x51, 0x97, 0x32, 0xb8, 0x81, 0x75, 0x76, 0xd6, 0xa1, 0x81, 0x12, 0x47, 0x30,
	0x0f, 0xb0, 0xed, 0x6f, 0x61, 0x6b, 0x9f, 0xe0, 0x6d, 0x80, 0xb0, 0xcc, 0x24, 0x33, 0xdb, 0xf7,
	0x2e, 0xc0, 0x73, 0x0c, 0xd6, 0xc8, 0xa1, 0xd2, 0x82, 0xe0, 0x7d, 0x80, 0x2d, 0x3f, 0x18, 0x2b,
	0x77, 0xa7, 0xc0, 0x87, 0x00, 0x9b, 0x3e, 0x9b, 0x85, 0x36, 0xdb, 0xc7, 0xd5, 0x31, 0xe3, 0x99,
	0x1c, 0x6a, 0xca, 0xb9, 0x26, 0x78, 0x1e, 0x56, 0xea, 0xf1, 0x4c, 0x76, 0x49, 0x4c, 0x63, 0x82,
	0x17, 0xe1, 0x7a, 0xeb, 0x41, 0x46, 0x5c, 0x17, 0x39, 0xbc, 0x0c, 0x71, 0x8f, 0xb5, 0x96, 0xb4,
	0x47, 0xc6, 0x3a, 0xf7, 0x65, 0xa2, 0xe0, 0xd5, 0x86, 0x89, 0x9f, 0xfa, 0xeb, 0xb0, 0x8a, 0x60,
	0xa1, 0xe0, 0x12, 0xde, 0x84, 0xd5, 0x0d, 0x0e, 0x34, 0x71, 0x43, 0x23, 0x2e, 0x53, 0x82, 0xef,
	0x51, 0x85, 0xcb, 0xf7, 0x28, 0xf1, 0x8f, 0x08, 0x2f, 0xb1, 0x3d, 0xff, 0xba, 0x32, 0xa5, 0xb1,
	0xe6, 0xf2, 0x38, 0x21, 0x3d, 0x20, 0x1e, 0x93, 0x86, 0x9f, 0x2b, 0x55, 0x39, 0xb2, 0x52, 0xf5,
	0x6b, 0x85, 0x7b, 0x64, 0x86, 0x44, 0xda, 0x45, 0xfc, 0x1d, 0xe1, 0x05, 0xd6, 0xf6, 0x5b, 0x61,
	0xee, 0xa9, 0x98, 0x06, 0x2a, 0x1d, 0xd0, 0x53, 0xca, 0xe0, 0x4f, 0x84, 0xe7, 0x59, 0xd3, 0xfd,
	0xba, 0x9f, 0x24, 0xd9, 0x54, 0x7a, 0xa7, 0xbf, 0x2b, 0xee, 0x97, 0xa0, 0xe4, 0xff, 0x22, 0x6c,
	0x30, 0xe6, 0xf8, 0xcd, 0xf8, 0xc9, 0x54, 0xc2, 0xe7, 0xc6, 0x2d, 0x98, 0x2f, 0x3a, 0xc1, 0xa7,
	0x45, 0x27, 0xf8, 0xba, 0xe8, 0x04, 0xcf, 0xbe, 0x75, 0xb6, 0x26, 0xdb, 0x6e, 0xf1, 0xf7, 0xff,
	0x07, 0x00, 0x00, 0xff, 0xff, 0x5b, 0xc3, 0xd6, 0x33, 0x26, 0x03, 0x00, 0x00,
}
