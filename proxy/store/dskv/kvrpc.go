package dskv

import (
	"fmt"
	"strconv"

	"model/pkg/errorpb"
	"model/pkg/kvrpcpb"
	"model/pkg/txn"
)

func EnumName(m map[int32]string, v int32) string {
	s, ok := m[v]
	if ok {
		return s
	}
	return strconv.Itoa(int(v))
}

type Type int32

const (
	Type_InvalidType Type = 0
	Type_RawGet      Type = 1
	Type_RawPut      Type = 2
	Type_RawDelete   Type = 3
	Type_RawExecute  Type = 4
	Type_Insert      Type = 5
	Type_Select      Type = 6
	Type_Delete      Type = 7
	Type_Update      Type = 8
	Type_KvSet       Type = 9
	Type_KvBatchSet  Type = 10
	Type_KvGet       Type = 11
	Type_KvBatchGet  Type = 12
	Type_KvScan      Type = 13
	Type_KvDelete    Type = 14
	Type_KvBatchDel  Type = 15
	Type_KvRangeDel  Type = 16
	Type_Lock        Type = 20
	Type_LockUpdate  Type = 21
	Type_Unlock      Type = 22
	Type_UnlockForce Type = 23
	Type_LockScan    Type = 24
	Type_TxPrepare   Type = 30
	Type_TxDecide    Type = 31
	Type_TxCleanup   Type = 32
	Type_TxGetLock   Type = 33
	Type_TxScan      Type = 34
)

var Type_name = map[int32]string{
	0:  "InvalidType",
	1:  "RawGet",
	2:  "RawPut",
	3:  "RawDelete",
	4:  "RawExecute",
	5:  "Insert",
	6:  "Select",
	7:  "Delete",
	8:  "Update",
	30: "TxPrepare",
	31: "TxDecide",
	32: "TxCleanup",
	33: "TxGetLock",
	34: "Type_TxScan",
}
var Type_value = map[string]int32{
	"InvalidType": 0,
	"RawGet":      1,
	"RawPut":      2,
	"RawDelete":   3,
	"RawExecute":  4,
	"Insert":      5,
	"Select":      6,
	"Delete":      7,
	"Update":      8,
	"TxPrepare":   30,
	"TxDecide":    31,
	"TxCleanup":   32,
	"TxGetLock":   33,
	"Type_TxScan": 34,
}

func (x Type) String() string {
	return EnumName(Type_name, int32(x))
}

type Request struct {
	Type          Type
	RawGetReq     *kvrpcpb.DsKvRawGetRequest
	RawPutReq     *kvrpcpb.DsKvRawPutRequest
	RawDeleteReq  *kvrpcpb.DsKvRawDeleteRequest
	RawExecuteReq *kvrpcpb.DsKvRawExecuteRequest

	SelectReq *kvrpcpb.DsSelectRequest
	InsertReq *kvrpcpb.DsInsertRequest
	DeleteReq *kvrpcpb.DsDeleteRequest
	UpdateReq *kvrpcpb.DsUpdateRequest

	LockReq        *kvrpcpb.DsLockRequest
	LockUpdateReq  *kvrpcpb.DsLockUpdateRequest
	UnlockReq      *kvrpcpb.DsUnlockRequest
	UnlockForceReq *kvrpcpb.DsUnlockForceRequest
	LockScanReq    *kvrpcpb.DsLockScanRequest

	TxPrepareReq *txnpb.DsPrepareRequest
	TxDecideReq  *txnpb.DsDecideRequest
	TxCleanupReq *txnpb.DsClearupRequest
	TxSelectReq  *txnpb.DsSelectRequest
	TxGetLockReq *txnpb.DsGetLockInfoRequest
	TxScanReq    *txnpb.DsScanRequest
}

func (m *Request) Reset() {
	*m = Request{}
}

func (m *Request) GetType() Type {
	if m != nil {
		return m.Type
	}
	return Type_InvalidType
}

func (m *Request) GetRawGetReq() *kvrpcpb.DsKvRawGetRequest {
	if m != nil {
		return m.RawGetReq
	}
	return nil
}

func (m *Request) GetLockReq() *kvrpcpb.DsLockRequest {
	if m != nil {
		return m.LockReq
	}
	return nil
}
func (m *Request) GetLockUpdateReq() *kvrpcpb.DsLockUpdateRequest {
	if m != nil {
		return m.LockUpdateReq
	}
	return nil
}
func (m *Request) GetUnlockReq() *kvrpcpb.DsUnlockRequest {
	if m != nil {
		return m.UnlockReq
	}
	return nil
}
func (m *Request) GetUnlockForceReq() *kvrpcpb.DsUnlockForceRequest {
	if m != nil {
		return m.UnlockForceReq
	}
	return nil
}
func (m *Request) GetLockScanReq() *kvrpcpb.DsLockScanRequest {
	if m != nil {
		return m.LockScanReq
	}
	return nil
}

func (m *Request) GetRawPutReq() *kvrpcpb.DsKvRawPutRequest {
	if m != nil {
		return m.RawPutReq
	}
	return nil
}

func (m *Request) GetRawDeleteReq() *kvrpcpb.DsKvRawDeleteRequest {
	if m != nil {
		return m.RawDeleteReq
	}
	return nil
}

func (m *Request) GetRawExecuteReq() *kvrpcpb.DsKvRawExecuteRequest {
	if m != nil {
		return m.RawExecuteReq
	}
	return nil
}

func (m *Request) GetSelectReq() *kvrpcpb.DsSelectRequest {
	if m != nil {
		return m.SelectReq
	}
	return nil
}

func (m *Request) GetInsertReq() *kvrpcpb.DsInsertRequest {
	if m != nil {
		return m.InsertReq
	}
	return nil
}

func (m *Request) GetDeleteReq() *kvrpcpb.DsDeleteRequest {
	if m != nil {
		return m.DeleteReq
	}
	return nil
}

func (m *Request) GetUpdateReq() *kvrpcpb.DsUpdateRequest {
	if m != nil {
		return m.UpdateReq
	}
	return nil
}

func (m *Request) GetTxPrepareReq() *txnpb.DsPrepareRequest {
	if m != nil {
		return m.TxPrepareReq
	}
	return nil
}

func (m *Request) GetTxDecideReq() *txnpb.DsDecideRequest {
	if m != nil {
		return m.TxDecideReq
	}
	return nil
}

func (m *Request) GetTxCleanupReq() *txnpb.DsClearupRequest {
	if m != nil {
		return m.TxCleanupReq
	}
	return nil
}

func (m *Request) GetTxSelectReq() *txnpb.DsSelectRequest {
	if m != nil {
		return m.TxSelectReq
	}
	return nil
}

func (m *Request) GetTxLockInfo() *txnpb.DsGetLockInfoRequest {
	if m != nil {
		return m.TxGetLockReq
	}
	return nil
}

func (m *Request) GetTxScanReq() *txnpb.DsScanRequest {
	if m != nil {
		return m.TxScanReq
	}
	return nil
}

type Response struct {
	Type           Type
	RawGetResp     *kvrpcpb.DsKvRawGetResponse
	RawPutResp     *kvrpcpb.DsKvRawPutResponse
	RawDeleteResp  *kvrpcpb.DsKvRawDeleteResponse
	RawExecuteResp *kvrpcpb.DsKvRawExecuteResponse

	SelectResp *kvrpcpb.DsSelectResponse
	InsertResp *kvrpcpb.DsInsertResponse
	DeleteResp *kvrpcpb.DsDeleteResponse
	UpdateResp *kvrpcpb.DsUpdateResponse

	LockResp        *kvrpcpb.DsLockResponse
	LockUpdateResp  *kvrpcpb.DsLockUpdateResponse
	UnlockResp      *kvrpcpb.DsUnlockResponse
	UnlockForceResp *kvrpcpb.DsUnlockForceResponse
	LockScanResp    *kvrpcpb.DsLockScanResponse

	TxPrepareResp *txnpb.DsPrepareResponse
	TxDecideResp  *txnpb.DsDecideResponse
	TxCleanupResp *txnpb.DsClearupResponse
	TxSelectResp  *txnpb.DsSelectResponse
	TxGetLockResp *txnpb.DsGetLockInfoResponse
	TxScanResp    *txnpb.DsScanResponse
}

func (m *Response) GetType() Type {
	if m != nil {
		return m.Type
	}
	return Type_InvalidType
}

func (m *Response) GetRawGetResp() *kvrpcpb.DsKvRawGetResponse {
	if m != nil {
		return m.RawGetResp
	}
	return nil
}

func (m *Response) GetRawPutResp() *kvrpcpb.DsKvRawPutResponse {
	if m != nil {
		return m.RawPutResp
	}
	return nil
}

func (m *Response) GetRawDeleteResp() *kvrpcpb.DsKvRawDeleteResponse {
	if m != nil {
		return m.RawDeleteResp
	}
	return nil
}

func (m *Response) GetRawExecuteResp() *kvrpcpb.DsKvRawExecuteResponse {
	if m != nil {
		return m.RawExecuteResp
	}
	return nil
}

func (m *Response) GetSelectResp() *kvrpcpb.DsSelectResponse {
	if m != nil {
		return m.SelectResp
	}
	return nil
}

func (m *Response) GetInsertResp() *kvrpcpb.DsInsertResponse {
	if m != nil {
		return m.InsertResp
	}
	return nil
}

func (m *Response) GetDeleteResp() *kvrpcpb.DsDeleteResponse {
	if m != nil {
		return m.DeleteResp
	}
	return nil
}

func (m *Response) GetUpdateResp() *kvrpcpb.DsUpdateResponse {
	if m != nil {
		return m.UpdateResp
	}
	return nil
}

func (m *Response) GetLockResp() *kvrpcpb.DsLockResponse {
	if m != nil {
		return m.LockResp
	}
	return nil
}
func (m *Response) GetLockUpdateResp() *kvrpcpb.DsLockUpdateResponse {
	if m != nil {
		return m.LockUpdateResp
	}
	return nil
}
func (m *Response) GetUnlockResp() *kvrpcpb.DsUnlockResponse {
	if m != nil {
		return m.UnlockResp
	}
	return nil
}
func (m *Response) GetUnlockForceResp() *kvrpcpb.DsUnlockForceResponse {
	if m != nil {
		return m.UnlockForceResp
	}
	return nil
}
func (m *Response) GetLockScanResp() *kvrpcpb.DsLockScanResponse {
	if m != nil {
		return m.LockScanResp
	}
	return nil
}

func (m *Response) GetTxPrepareResp() *txnpb.DsPrepareResponse {
	if m != nil {
		return m.TxPrepareResp
	}
	return nil
}

func (m *Response) GetTxDecideResp() *txnpb.DsDecideResponse {
	if m != nil {
		return m.TxDecideResp
	}
	return nil
}

func (m *Response) GetTxCleanupResp() *txnpb.DsClearupResponse {
	if m != nil {
		return m.TxCleanupResp
	}
	return nil
}

func (m *Response) GetTxSelectResp() *txnpb.DsSelectResponse {
	if m != nil {
		return m.TxSelectResp
	}
	return nil
}

func (m *Response) GetTxLockInfoResp() *txnpb.DsGetLockInfoResponse {
	if m != nil {
		return m.TxGetLockResp
	}
	return nil
}

func (m *Response) GetTxScanResp() *txnpb.DsScanResponse {
	if m != nil {
		return m.TxScanResp
	}
	return nil
}

func (resp *Response) GetErr() (pErr *errorpb.Error, err error) {
	switch resp.Type {
	case Type_RawPut:
		pErr = resp.RawPutResp.GetHeader().GetError()
	case Type_RawGet:
		pErr = resp.RawGetResp.GetHeader().GetError()
	case Type_RawDelete:
		pErr = resp.RawDeleteResp.GetHeader().GetError()
	case Type_Insert:
		pErr = resp.InsertResp.GetHeader().GetError()
	case Type_Update:
		pErr = resp.UpdateResp.GetHeader().GetError()
	case Type_Select:
		pErr = resp.SelectResp.GetHeader().GetError()
	case Type_Delete:
		pErr = resp.DeleteResp.GetHeader().GetError()
	case Type_Lock:
		pErr = resp.LockResp.GetHeader().GetError()
	case Type_LockUpdate:
		pErr = resp.LockUpdateResp.GetHeader().GetError()
	case Type_Unlock:
		pErr = resp.UnlockResp.GetHeader().GetError()
	case Type_UnlockForce:
		pErr = resp.UnlockForceResp.GetHeader().GetError()
	case Type_LockScan:
		pErr = resp.LockScanResp.GetHeader().GetError()
	case Type_TxPrepare:
		pErr = resp.TxPrepareResp.GetHeader().GetError()
	case Type_TxDecide:
		pErr = resp.TxDecideResp.GetHeader().GetError()
	case Type_TxCleanup:
		pErr = resp.TxCleanupResp.GetHeader().GetError()
	case Type_TxGetLock:
		pErr = resp.TxGetLockResp.GetHeader().GetError()
	case Type_TxScan:
		pErr = resp.TxScanResp.GetHeader().GetError()
	default:
		err = fmt.Errorf("invalid response type %s", resp.Type.String())
	}
	return
}
