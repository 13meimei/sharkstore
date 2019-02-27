package server

import (
	"fmt"
	"errors"
	"pkg-go/ds_client"
	"proxy/store/dskv"
	"model/pkg/txn"
	"util/log"
)

func (p *Proxy) handlePrepare(ctx *dskv.ReqContext, req *txnpb.PrepareRequest, t *Table) (err error) {
	if len(req.GetIntents()) == 0 {
		return
	}
	var (
		resp        *txnpb.PrepareResponse
		errForRetry error
	)
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	/**
		retry and wait for exist lock to expire
	 */
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("handlePrepare [%s] execute timeout", ctx)
				return
			}
		}
		resp, err = proxy.TxPrepare(ctx, req, req.GetIntents()[0].GetKey())
		if err != nil {
			return
		}
		if len(resp.Errors) > 0 {
			expiredTxs := make([]*txnpb.LockError, 0)
			for _, txError := range resp.GetErrors() {
				switch txError.GetErrType() {
				case txnpb.TxnError_LOCKED:
					lockErr := txError.GetLockErr()
					if lockErr.GetInfo() != nil && lockErr.GetInfo().GetTimeout() {
						expiredTxs = append(expiredTxs, lockErr)
					}
					log.Info("handlePrepare: txn[%v] exist lock[%v], need wait to expire", req.GetTxnId(), lockErr.GetInfo())
				default:
					err = convertTxnErr(txError)
					log.Error("handlePrepare txn[%v] error ", req.GetTxnId(), err)
					return
				}
			}
			//recover expired txs
			if err = p.handleRecoverTxs(expiredTxs, t); err != nil {
				return
			}
			continue
		}
		return
	}
}

/**
	recover(commit or rollback) previous expired transaction
 */
func (p *Proxy) handleRecoverTxs(expiredTxs []*txnpb.LockError, t *Table) (err error) {
	if len(expiredTxs) == 0 {
		return
	}
	var errChannel = make(chan error, len(expiredTxs))
	defer close(errChannel)
	recoverFunc := func(subCtx *dskv.ReqContext, lock *txnpb.LockError) {
		var (
			lockInfo   = lock.GetInfo()
			txId       = lockInfo.GetTxnId()
			primaryKey = lockInfo.GetPrimaryKey()
			e          error
		)
		if lock.GetInfo().IsPrimary {
			e = p.handleRecoverPrimary(subCtx, txId, primaryKey, lockInfo.GetSecondaryKeys(), false, t)
		} else {
			e = p.handleRecoverSecondary(subCtx, txId, primaryKey, t)
		}
		if e != nil {
			log.Error("recover expired tx %v err %v", txId, err)
		}
		errChannel <- e
	}
	ctx := dskv.NewPRConext(int(TXN_DEFAULT_TIMEOUT * 1000))
	for _, expiredTx := range expiredTxs {
		cClone := ctx.Clone()
		go recoverFunc(cClone, expiredTx)
	}
	var errCount int
	for i := 0; i < len(expiredTxs); i++ {
		if e := <-errChannel; e != nil {
			errCount++
			err = e
		}
	}
	if errCount > 0 {
		return fmt.Errorf("batch recover expired txs err, errorCount %v", errCount)
	}
	return
}

func (p *Proxy) handleRecoverPrimary(ctx *dskv.ReqContext, txId string, primaryKey []byte,
	secondaryKeys [][]byte, recover bool, t *Table) (err error) {

	var status = txnpb.TxnStatus_ABORTED
	//first try to decide expired tx to aborted status
	if recover {
		status, secondaryKeys, err = p.tryRollbackTxn(ctx, txId, primaryKey, recover, t)
	} else {
		status, _, err = p.tryRollbackTxn(ctx, txId, primaryKey, recover, t)
	}
	if err != nil {
		return
	}
	if status == txnpb.TxnStatus_COMMITTED {
		log.Error("rollback txn error, because ds let commit")
	}
	//todo opt
	//decide all secondary keys
	err = p.decideSecondaryKeys(ctx, txId, status, secondaryKeys, t)
	if err != nil {
		return
	}
	//decide primary key
	err = p.decidePrimaryKey(ctx, txId, status, primaryKey, t)
	if err != nil {
		return
	}
	//clear up
	return p.handleCleanup(ctx, txId, primaryKey, t)
}

func (p *Proxy) handleRecoverSecondary(ctx *dskv.ReqContext, txId string, primaryKey []byte, t *Table) (err error) {
	var (
		status        = txnpb.TxnStatus_ABORTED
		secondaryKeys [][]byte
	)
	//first try to decide expired tx to aborted status
	status, secondaryKeys, err = p.tryRollbackTxn(ctx, txId, primaryKey, true, t)
	if err != nil {
		return
	}
	if status == txnpb.TxnStatus_COMMITTED {
		log.Error("rollback txn error, because ds let commit")
	}
	//todo opt
	//decide all secondary keys
	err = p.decideSecondaryKeys(ctx, txId, status, secondaryKeys, t)
	if err != nil {
		return
	}
	//decide primary key
	err = p.decidePrimaryKey(ctx, txId, status, primaryKey, t)
	if err != nil {
		return
	}
	//clear up
	return p.handleCleanup(ctx, txId, primaryKey, t)
}

func (p *Proxy) asyncRecoverSecondary(ctx *dskv.ReqContext, txId string, primaryKey []byte, t *Table) (txnpb.TxnStatus, error) {
	var (
		status        = txnpb.TxnStatus_ABORTED
		err           error
		secondaryKeys [][]byte
	)
	//first try to decide expired tx to aborted status
	status, secondaryKeys, err = p.tryRollbackTxn(ctx, txId, primaryKey, true, t)
	if err != nil {
		return status, err
	}
	if status == txnpb.TxnStatus_COMMITTED {
		log.Error("rollback txn error, because ds let commit")
	}
	//todo opt
	go func(p *Proxy, txId string, primaryKey []byte, sta txnpb.TxnStatus) {
		//decide all secondary keys
		err = p.decideSecondaryKeys(ctx, txId, sta, secondaryKeys, t)
		if err != nil {
			return
		}
		//decide primary key
		err = p.decidePrimaryKey(ctx, txId, sta, primaryKey, t)
		if err != nil {
			return
		}
		//clear up
		p.handleCleanup(ctx, txId, primaryKey, t)
	}(p, txId, primaryKey, status)
	return status, err
}

func (p *Proxy) decidePrimaryKey(ctx *dskv.ReqContext, txId string, status txnpb.TxnStatus, primaryKey []byte, t *Table) (err error) {
	if len(primaryKey) == 0 {
		return errors.New("primary key is empty")
	}
	var (
		errForRetry error
		req         *txnpb.DecideRequest
		resp        *txnpb.DecideResponse
	)
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[recover]%s execute timeout", ctx)
				return
			}
		}
		req = &txnpb.DecideRequest{
			TxnId:  txId,
			Status: status,
			Keys:   [][]byte{primaryKey},
		}
		//todo re find route
		resp, err = p.handleDecide(ctx, req, t)
		if err != nil {
			if err == dskv.ErrRouteChange {
				errForRetry = err
				continue
			}
			return
		}
		if resp.GetErr() != nil {
			err = convertTxnErr(resp.Err)
		}
		return
	}
}

func (p *Proxy) decideSecondaryKeys(ctx *dskv.ReqContext, txId string, status txnpb.TxnStatus, secondaryKeys [][]byte, t *Table) (err error) {
	if len(secondaryKeys) == 0 {
		return
	}
	var (
		errForRetry error
		req         *txnpb.DecideRequest
		resp        *txnpb.DecideResponse
	)
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[recover]%s execute timeout", ctx)
				return
			}
		}
		//todo re find route
		req = &txnpb.DecideRequest{
			TxnId:  txId,
			Status: status,
			Keys:   secondaryKeys,
		}
		resp, err = p.handleDecide(ctx, req, t)
		if err != nil {
			if err == dskv.ErrRouteChange {
				errForRetry = err
				continue
			}
			return
		}
		if resp.Err != nil {
			err = convertTxnErr(resp.Err)
			log.Error("handlePrepare txn[%v] error ", req.GetTxnId(), err)
		}
		return
	}
}

func (p *Proxy) tryRollbackTxn(ctx *dskv.ReqContext, txId string, primaryKey []byte, recover bool, t *Table) (
	status txnpb.TxnStatus, secondaryKeys [][]byte, err error) {

	status = txnpb.TxnStatus_ABORTED
	var (
		req = &txnpb.DecideRequest{
			TxnId:  txId,
			Status: status,
			Keys:   [][]byte{primaryKey},
		}
		resp        *txnpb.DecideResponse
		errForRetry error
	)
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[recover]%s execute timeout", ctx)
				return
			}
		}
		resp, err = p.handleDecide(ctx, req, t)
		if err != nil {
			if err == dskv.ErrRouteChange {
				errForRetry = err
				continue
			}
			return
		}
		if resp.GetErr() != nil {
			switch resp.Err.GetErrType() {
			case txnpb.TxnError_STATUS_CONFLICT:
				//failure, commit
				status = txnpb.TxnStatus_COMMITTED
				log.Info("handleRecover: txn[%v] retry to aborted and return status conflict", txId)
			default:
				err = convertTxnErr(resp.Err)
				return
			}
		}
		return
	}
}

func (p *Proxy) handleDecide(ctx *dskv.ReqContext, req *txnpb.DecideRequest, t *Table) (*txnpb.DecideResponse, error) {
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)
	resp, err := proxy.TxDecide(ctx, req, req.GetKeys()[0])
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Proxy) handleCleanup(ctx *dskv.ReqContext, txId string, primaryKey []byte, t *Table) (err error) {
	req := &txnpb.ClearupRequest{
		TxnId:      txId,
		PrimaryKey: primaryKey,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	var (
		resp        *txnpb.ClearupResponse
		errForRetry error
	)
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[clearup]%s execute timeout", ctx)
				return
			}
		}
		resp, err = proxy.TxCleanup(ctx, req, primaryKey)
		if err != nil {
			if err == dskv.ErrRouteChange {
				errForRetry = err
				continue
			}
			return
		}
		if resp.GetErr() != nil {
			err = convertTxnErr(resp.Err)
		}
		return
	}
}

func (p *Proxy) handleGetLockInfo(ctx *dskv.ReqContext, txId string, primaryKey []byte, t *Table) (resp *txnpb.GetLockInfoResponse, err error) {
	req := &txnpb.GetLockInfoRequest{
		TxnId: txId,
		Key:   primaryKey,
	}
	proxy := dskv.GetKvProxy()
	defer dskv.PutKvProxy(proxy)
	proxy.Init(p.dsCli, p.clock, t.ranges, client.WriteTimeout, client.ReadTimeoutShort)

	var errForRetry error
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[getLockInfo]%s execute timeout", ctx)
				return
			}
		}
		resp, err = proxy.TxGetLock(ctx, req, req.GetKey())
		if err != nil && err == dskv.ErrRouteChange {
			errForRetry = err
			continue
		}
		return
	}
}

func convertTxnErr(txError *txnpb.TxnError) error {
	var err error
	switch txError.GetErrType() {
	case txnpb.TxnError_SERVER_ERROR:
		serverErr := txError.GetServerErr()
		err = fmt.Errorf("SERVER_ERROR, code:[%v], message:%v", serverErr.GetCode(), serverErr.GetMsg())
	case txnpb.TxnError_LOCKED:
		lockErr := txError.GetLockErr()
		err = fmt.Errorf("TXN EXSIST LOCKED, lockTxId: %v, timeout:%v", lockErr.GetInfo().GetTxnId(), lockErr.GetInfo().GetTxnId())
	case txnpb.TxnError_UNEXPECTED_VER:
		versionErr := txError.GetUnexpectedVer()
		err = fmt.Errorf("UNEXPECTED VERSION, expectedVer: %v, actualVer:%v", versionErr.GetExpectedVer(), versionErr.GetActualVer())
	case txnpb.TxnError_STATUS_CONFLICT:
		statusConflict := txError.GetStatusConflict()
		err = fmt.Errorf("TXN STATUS CONFLICT, status: %v", statusConflict.GetStatus())
	case txnpb.TxnError_NOT_FOUND:
		err = fmt.Errorf("NOT FOUND")
	case txnpb.TxnError_NOT_UNIQUE:
		err = fmt.Errorf("NOT UNIQUE")
	default:
		err = fmt.Errorf("UNKNOWN TXN Err")
	}
	return err
}

type TxnDealHandle struct {
	intents []*txnpb.TxnIntent
	err     error
}
