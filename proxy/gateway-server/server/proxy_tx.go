package server

import (
	"fmt"
	"bytes"
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
					log.Error("handlePrepare txn[%v] error:%v ", req.GetTxnId(), err)
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
	recoverFunc := func(subCtx *dskv.ReqContext, lock *txnpb.LockError, table *Table, errs chan error) {
		var (
			lockInfo   = lock.GetInfo()
			txId       = lockInfo.GetTxnId()
			primaryKey = lockInfo.GetPrimaryKey()
			e          error
		)
		if lockInfo.GetIsPrimary() {
			e = p.recoverFromPrimary(subCtx, txId, primaryKey, lockInfo.GetSecondaryKeys(), false, t)
		} else {
			_, e = p.recoverFromSecondary(subCtx, txId, primaryKey, table, true)
		}
		if e != nil {
			log.Error("recover expired tx %v err %v", txId, err)
		}
		errs <- e
	}
	var errChannel = make(chan error, len(expiredTxs))
	defer close(errChannel)
	ctx := dskv.NewPRConext(int(TXN_DEFAULT_TIMEOUT * 1000))
	for _, expiredTx := range expiredTxs {
		cClone := ctx.Clone()
		go recoverFunc(cClone, expiredTx, t, errChannel)
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

func (p *Proxy) recoverFromPrimary(ctx *dskv.ReqContext, txId string, primaryKey []byte,
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
	//clear up
	return p.handleCleanup(ctx, txId, primaryKey, t)
}

func (p *Proxy) recoverFromSecondary(ctx *dskv.ReqContext, txId string, primaryKey []byte, t *Table, sync bool) (txnpb.TxnStatus, error) {
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
	if sync {
		//todo opt
		//decide all secondary keys
		err = p.decideSecondaryKeys(ctx, txId, status, secondaryKeys, t)
		if err != nil {
			return status, err
		}
		//clear up
		return status, p.handleCleanup(ctx, txId, primaryKey, t)
	}
	//todo opt
	go func(p *Proxy, txId string, primaryKey []byte, sta txnpb.TxnStatus) {
		//decide all secondary keys
		if e := p.decideSecondaryKeys(ctx, txId, sta, secondaryKeys, t); e != nil {
			return
		}
		//clear up
		p.handleCleanup(ctx, txId, primaryKey, t)
	}(p, txId, primaryKey, status)
	return status, err
}

func (p *Proxy) decideSecondaryKeys(ctx *dskv.ReqContext, txId string, status txnpb.TxnStatus, secondaryKeys [][]byte, t *Table) (err error) {
	if len(secondaryKeys) == 0 {
		return
	}
	var (
		handleKeys     = secondaryKeys
		handleKeyGroup [][][]byte
		needRetryKeys  [][]byte
		errForRetry    error
	)
	doDecideFunc := func(proxy *Proxy, subCtx *dskv.ReqContext, txId string, subKeys [][]byte, table *Table, handleChannel chan *TxnDealHandle) {
		log.Debug("[recover]doDecideFunc: decide tx %v secondary key %v", txId, subKeys)
		req := &txnpb.DecideRequest{
			TxnId:  txId,
			Status: status,
			Keys:   subKeys,
		}
		var resp *txnpb.DecideResponse
		resp, err = proxy.handleDecide(subCtx, req, table)
		if err != nil {
			handleChannel <- &TxnDealHandle{keys: subKeys, err: err}
		}
		if resp.Err != nil {
			err = convertTxnErr(resp.Err)
			handleChannel <- &TxnDealHandle{keys: subKeys, err: err}
			return
		}
		handleChannel <- &TxnDealHandle{keys: subKeys, err: nil}
	}
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[recover]%s execute secondary intents timeout", ctx)
				return
			}
		}
		handleKeyGroup, err = regroupKeysByRange(ctx, t, handleKeys)
		if err != nil {
			return
		}
		handleGroup := len(handleKeyGroup)
		var handleChannel = make(chan *TxnDealHandle, handleGroup)
		for _, group := range handleKeyGroup {
			cClone := ctx.Clone()
			go doDecideFunc(p, cClone, txId, group, t, handleChannel)
		}
		for i := 0; i < handleGroup; i++ {
			txnHandle := <-handleChannel
			if txnHandle.err != nil {
				err = txnHandle.err
				if err == dskv.ErrRouteChange {
					needRetryKeys = append(needRetryKeys, txnHandle.keys...)
				} else {
					close(handleChannel)
					return
				}
			}
		}
		close(handleChannel)
		if len(needRetryKeys) > 0 {
			errForRetry = dskv.ErrRouteChange
			continue
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
			return
		}
		log.Debug("clear up txn[%v] primary intent success", txId)
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
	keys    [][]byte
	err     error
}

type TxnIntentSlice []*txnpb.TxnIntent

func (t TxnIntentSlice) Len() int {
	return len(t)
}

func (t TxnIntentSlice) Swap(i int, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TxnIntentSlice) Less(i int, j int) bool {
	return bytes.Compare(t[i].GetKey(), t[j].GetKey()) < 0
}

// 按照route的范围划分intents
func regroupIntentsByRange(context *dskv.ReqContext, t *Table, intents []*txnpb.TxnIntent) ([]*txnpb.TxnIntent, [][]*txnpb.TxnIntent, error) {
	var (
		priIntentsGroup []*txnpb.TxnIntent
		secIntentsGroup [][]*txnpb.TxnIntent
		err             error
		pkRangeId       uint64
	)
	ggroup := make(map[uint64][]*txnpb.TxnIntent)
	for _, intent := range intents {
		var (
			l     *dskv.KeyLocation
			group []*txnpb.TxnIntent
			ok    bool
		)
		l, err = t.ranges.LocateKey(context.GetBackOff(), intent.GetKey())
		if err != nil {
			log.Warn("[regroupIntents]locate key failed, err %v", err)
			return nil, nil, err
		}
		if intent.GetIsPrimary() {
			pkRangeId = l.Region.Id
		}
		if group, ok = ggroup[l.Region.Id]; !ok {
			group = make([]*txnpb.TxnIntent, 0)
			ggroup[l.Region.Id] = group
		}
		group = append(group, intent)
		ggroup[l.Region.Id] = group
	}
	for rangeId, group := range ggroup {
		if len(group) == 0 {
			continue
		}
		if rangeId == pkRangeId && len(priIntentsGroup) == 0 {
			priIntentsGroup = group
		} else {
			secIntentsGroup = append(secIntentsGroup, group)
		}
	}
	return priIntentsGroup, secIntentsGroup, nil
}

// 按照route的范围划分keys
func regroupKeysByRange(context *dskv.ReqContext, t *Table, keys [][]byte) ([][][]byte, error) {
	var (
		keysGroup [][][]byte
		err       error
	)
	ggroup := make(map[uint64][][]byte)
	for _, key := range keys {
		var (
			l     *dskv.KeyLocation
			group [][]byte
			ok    bool
		)
		l, err = t.ranges.LocateKey(context.GetBackOff(), key)
		if err != nil {
			log.Warn("[regroupIntents]locate key failed, err %v", err)
			return nil, err
		}
		if group, ok = ggroup[l.Region.Id]; !ok {
			group = make([][]byte, 0)
			ggroup[l.Region.Id] = group
		}
		group = append(group, key)
		ggroup[l.Region.Id] = group
	}
	for _, group := range ggroup {
		if len(group) == 0 {
			continue
		}
		keysGroup = append(keysGroup, group)
	}
	return keysGroup, nil
}
