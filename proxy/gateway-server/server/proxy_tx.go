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
		resp, err = proxy.TxPrepare(ctx, req, nil)
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
			if err = p.handleRecoverExpiredTxs(expiredTxs); err != nil {
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
func (p *Proxy) handleRecoverExpiredTxs(expiredTxs []*txnpb.LockError) (err error) {
	if len(expiredTxs) == 0 {
		return
	}
	var errChannel = make(chan error, len(expiredTxs))
	defer close(errChannel)
	recoverFunc := func(txnLock *txnpb.LockError) {
		var (
			lockInfo      = txnLock.GetInfo()
			primaryKey    = lockInfo.GetPrimaryKey()
			isPrimary     = lockInfo.IsPrimary
			status        = txnpb.TxnStatus_ABORTED
			ctx           = dskv.NewPRConext(int(TXN_DEFAULT_TIMEOUT * 1000))
			secondaryKeys [][]byte
		)
		//first try to decide expired tx to aborted status
		secondaryKeys, err = p.tryRollbackTxn(ctx, lockInfo.GetTxnId(), &status, lockInfo.GetPrimaryKey(), isPrimary)
		if err != nil {
			errChannel <- err
			return
		}
		if isPrimary {
			secondaryKeys = txnLock.GetInfo().GetSecondaryKeys()
		}
		//todo opt
		//decide all secondary keys
		err = p.decideSecondaryKeys(ctx, lockInfo.GetTxnId(), status, secondaryKeys, nil)
		if err != nil {
			errChannel <- err
			return
		}
		//decide primary key
		err = p.decidePrimaryKey(ctx, lockInfo.GetTxnId(), status, primaryKey, nil)
		if err != nil {
			errChannel <- err
			return
		}
		//clear up
		err = p.handleCleanup(ctx, lockInfo.GetTxnId(), primaryKey, nil)
		if err != nil {
			errChannel <- err
			return
		}
	}

	for _, expiredTx := range expiredTxs {
		go recoverFunc(expiredTx)
	}
	return
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
		resp, err = p.handleDecide(ctx, req, nil)
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
		resp, err = p.handleDecide(ctx, req, nil)
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

func (p *Proxy) tryRollbackTxn(ctx *dskv.ReqContext, txId string, status *txnpb.TxnStatus, primaryKey []byte, recover bool) (
	secondaryKeys [][]byte, err error) {
	var (
		req = &txnpb.DecideRequest{
			TxnId:  txId,
			Status: *status,
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
		resp, err = p.handleDecide(ctx, req, nil)
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
				*status = txnpb.TxnStatus_COMMITTED
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
		resp *txnpb.ClearupResponse
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


func handleSecondary(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent,
	handleFunc func(*dskv.ReqContext, []*txnpb.TxnIntent, chan *TxnDealHandle)) (err error) {
	var (
		finalSecIntents  = make([][]*txnpb.TxnIntent, 0)
		handleIntents    = secIntents
		needRetryIntents []*txnpb.TxnIntent
		errForRetry      error
	)
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[commit]%s execute secondary intents timeout", ctx)
				return
			}
		}
		if err == dskv.ErrRouteChange {
			_, handleIntents, err = regroupIntentsByRange(ctx, nil, needRetryIntents)
			if err != nil {
				return
			}
		}
		handleGroup := len(handleIntents)
		var handleChannel = make(chan *TxnDealHandle, handleGroup)
		for _, group := range handleIntents {
			cClone := ctx.Clone()
			go handleFunc(cClone, group, handleChannel)
		}
		for i := 0; i < handleGroup; i++ {
			txnHandle := <-handleChannel
			if txnHandle.err != nil {
				if txnHandle.err == dskv.ErrRouteChange {
					needRetryIntents = append(needRetryIntents, txnHandle.intents...)
				} else {
					close(handleChannel)
					return
				}
			} else {
				finalSecIntents = append(finalSecIntents, txnHandle.intents)
			}
		}
		close(handleChannel)
		if len(needRetryIntents) > 0 {
			errForRetry = dskv.ErrRouteChange
			continue
		}
		break
	}
	secIntents = finalSecIntents
	return
}

func convertTxnErr(txError *txnpb.TxnError) error {
	var err error
	switch txError.GetErrType() {
	case txnpb.TxnError_SERVER_ERROR:
		err = fmt.Errorf("SERVER_ERROR")
	case txnpb.TxnError_LOCKED:
		err = fmt.Errorf("TXN EXSIST LOCKED")
	case txnpb.TxnError_UNEXPECTED_VER:
		err = fmt.Errorf("UNEXPECTED VERSION")
	case txnpb.TxnError_STATUS_CONFLICT:
		err = fmt.Errorf("TXN STATUS CONFLICT")
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
