package server

import (
	"fmt"
	"sort"
	"util"
	"util/log"
	"model/pkg/txn"
	"proxy/store/dskv"
)

type TX interface {
	GetTxId() string
	IsImplicit() bool
	IsLocal() bool
	GetPrimaryKey() []byte
	SetTable(table *Table)
	GetTable() *Table

	Insert([]*txnpb.TxnIntent) error
	Update([]*txnpb.TxnIntent) error
	Select([]*txnpb.TxnIntent) error
	Delete([]*txnpb.TxnIntent) error
	Commit() error
	Rollback() error
}

type TxObj struct {
	txId       string
	implicit   bool
	local      bool
	primaryKey []byte
	status     txnpb.TxnStatus
	intents    []*txnpb.TxnIntent
	//todo opt to multiple table
	//intents on table level
	//tabIntents map[string][]*txnpb.TxnIntent
	table *Table
	proxy *Proxy

	Timeout uint64
}

func NewTx(implicit bool, p *Proxy, timeout uint64) TX {
	txId := util.NewUuid()
	tx := &TxObj{txId: txId, proxy: p, implicit: implicit}
	ttl := TXN_DEFAULT_TIMEOUT
	if timeout > 0 {
		ttl = timeout
	}
	tx.Timeout = ttl
	return tx
}

func (t *TxObj) GetTxId() string {
	if t != nil {
		return t.txId
	}
	return ""
}

func (t *TxObj) IsImplicit() bool {
	if t != nil {
		return t.implicit
	}
	return false
}

func (t *TxObj) IsLocal() bool {
	if t != nil {
		return t.local
	}
	return false
}

func (t *TxObj) GetPrimaryKey() []byte {
	if t != nil {
		return t.primaryKey
	}
	return nil
}

func (t *TxObj) SetTable(table *Table) {
	if t == nil {
		return
	}
	t.table = table
}

func (t *TxObj) GetTable() *Table {
	if t == nil {
		return nil
	}
	return t.table
}

func (t *TxObj) getTxIntents() []*txnpb.TxnIntent {
	if t != nil {
		return t.intents
	}
	return nil
}

func (t *TxObj) Insert(intents []*txnpb.TxnIntent) (err error) {
	if len(intents) == 0 {
		log.Info("[txn]insert intent is empty")
		return
	}
	for i, intent := range intents {
		if i == 0 && len(t.GetPrimaryKey()) == 0 {
			intent.IsPrimary = true
			t.primaryKey = intent.GetKey()
		}
		t.intents = append(t.intents, intent)
	}
	log.Debug("tx %v cache inserted intents success", t.GetTxId())
	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			log.Error("tx %v commit implicitly failed, err: %v", t.GetTxId(), err)
			if e := t.Rollback(); e != nil {
				log.Error("tx %v rollback implicitly failed, err: %v", t.GetTxId(), err)
			}
			return
		}
		log.Debug("tx %v commit implicitly success", t.GetTxId())
	}
	return
}

func (t *TxObj) Update(intents []*txnpb.TxnIntent) (err error) {
	if len(intents) == 0 {
		log.Info("[txn]update intent is empty")
		return
	}
	//todo merge previous txnIntent: the same transaction should be visible
	for i, intent := range intents {
		if i == 0 && len(t.GetPrimaryKey()) == 0 {
			intent.IsPrimary = true
			t.primaryKey = intent.GetKey()
		}
		t.intents = append(t.intents, intent)
	}
	log.Debug("tx %v cache updated intents success", t.GetTxId())
	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			log.Error("tx %v commit implicitly failed, err: %v", t.GetTxId(), err)
			if e := t.Rollback(); e != nil {
				log.Error("tx %v rollback implicitly failed, err: %v", t.GetTxId(), err)
			}
			return
		}
		log.Debug("tx %v commit implicitly success", t.GetTxId())
	}
	return
}

func (t *TxObj) Select(intents []*txnpb.TxnIntent) (err error) {
	return
}

func (t *TxObj) Delete(intents []*txnpb.TxnIntent) (err error) {
	if len(intents) == 0 {
		log.Info("[txn]delete intent is empty")
		return
	}
	for i, intent := range intents {
		if i == 0 && len(t.GetPrimaryKey()) == 0 {
			intent.IsPrimary = true
			t.primaryKey = intent.GetKey()
		}
		t.intents = append(t.intents, intent)
	}
	log.Debug("tx %v cache deleted intents success", t.GetTxId())
	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			log.Error("tx %v commit implicitly failed, err: %v", t.GetTxId(), err)
			if e := t.Rollback(); e != nil {
				log.Error("tx %v rollback implicitly failed, err: %v", t.GetTxId(), err)
			}
			return
		}
		log.Debug("tx %v commit implicitly success", t.GetTxId())
	}
	return
}

/**
	commit:  prepare and decide 2PL duration tx timeout (if tx is local, optimize to 1PL)
	prepare: first write primary intents, second write secondary intents
	decide:  decide primary intents, then async to decide secondary intents, last clear tx record
 */
func (t *TxObj) Commit() (err error) {
	var (
		passed bool
		status = txnpb.TxnStatus_COMMITTED
	)
	passed, err = t.changeTxStatus(status)
	if err != nil || !passed {
		return
	}
	if len(t.intents) == 0 {
		log.Info("[commit] tx %v intent is empty", t.GetTxId())
		return
	}
	ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	var (
		priIntents      []*txnpb.TxnIntent
		secIntentsGroup [][]*txnpb.TxnIntent
	)
	sort.Sort(TxnIntentSlice(t.intents))
	priIntents, secIntentsGroup, err = regroupIntentsByRange(ctx, t.GetTable(), t.intents)
	if err != nil {
		return
	}
	/**
	  func: prepare primary row intents
	  priIntentsGroup and secIntentsGroup size are affected by priIntentsGroup prepare result,
	  so they can be changed at func 'preparePrimaryIntents'(reference)
	 */
	err = t.preparePrimaryIntents(ctx, priIntents, secIntentsGroup)
	if err != nil {
		log.Error("[commit]prepare tx %v primary intents error %v", t.GetTxId(), err)
		return
	}
	//todo local txn optimize to 1PL
	//if !t.IsLocal() {
	//concurrency prepare secondary row intents
	err = t.prepareSecondaryIntents(ctx, secIntentsGroup)
	if err != nil {
		log.Error("[commit]prepare tx %v secondary intents error %v", t.GetTxId(), err)
		return
	}

	//decide:
	err = t.decidePrimaryIntents(ctx, priIntents, secIntentsGroup, status)
	if err != nil {
		log.Error("[commit]decide tx %v primary intents error %v", t.GetTxId(), err)
		return
	}
	//async call
	//todo goroutine num control
	go func(intents [][]*txnpb.TxnIntent) {
		var e error
		if e = t.decideSecondaryIntents(intents, status); e != nil {
			log.Warn("async decide txn %v secondary intent error %v", t.GetTxId(), e)
			return
		}
		if e = t.proxy.handleCleanup(ctx, t.GetTxId(), t.GetPrimaryKey(), t.GetTable()); e != nil {
			log.Warn("async decide txn %v secondary intent error %v", t.GetTxId(), e)
		}
		return
	}(secIntentsGroup)
	//}
	log.Info("txn %v commit success", t.GetTxId())
	return
}

/**
    rollback current transaction
 */
func (t *TxObj) Rollback() (err error) {
	var passed bool
	passed, err = t.changeTxStatus(txnpb.TxnStatus_ABORTED)
	if err != nil || !passed {
		return
	}
	if len(t.intents) == 0 {
		return
	}
	ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	err = t.proxy.recoverFromPrimary(ctx, t.GetTxId(), t.GetPrimaryKey(), nil, true, t.GetTable())
	if err != nil {
		log.Error("txn %v rollback err %v", t.GetTxId(), err)
		return
	}
	log.Info("txn %v rollback success", t.GetTxId())
	return
}

func (t *TxObj) preparePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent, secIntents [][]*txnpb.TxnIntent) (err error) {

	var (
		req = &txnpb.PrepareRequest{
			TxnId:         t.GetTxId(),
			PrimaryKey:    t.GetPrimaryKey(),
			LockTtl:       t.Timeout,
			SecondaryKeys: t.getSecondaryKeys(),
		}
		errForRetry error
	)
	/**
		loop solve: occur ErrRouteChange when prepare intents with primary row, regroup intents
	 */
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[commit]%s execute prepare primary intents timeout", ctx)
				return
			}
		}

		if err == dskv.ErrRouteChange {
			var partSecIntents [][]*txnpb.TxnIntent
			priIntents, partSecIntents, err = regroupIntentsByRange(ctx, t.GetTable(), priIntents)
			if err != nil || len(priIntents) == 0 {
				return
			}
			if len(partSecIntents) > 0 {
				secIntents = append(secIntents, partSecIntents...)
			}
		}

		isLocal := isLocalTxn(priIntents, secIntents)
		req.Local = isLocal
		req.Intents = priIntents

		log.Debug("start to prepare tx %v primary intents, local: %v, intents size: %v", t.GetTxId(), isLocal, len(priIntents))
		err = t.proxy.handlePrepare(ctx, req, t.GetTable())
		if err != nil {
			if err == dskv.ErrRouteChange {
				log.Warn("prepare tx %v primary intents router change, retry")
				errForRetry = err
				continue
			}
			return err
		}
		t.local = isLocal
		log.Debug("prepare tx %v primary intents success", t.GetTxId())
		return
	}
}

func (t *TxObj) prepareSecondaryIntents(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent) (err error) {
	log.Debug("start to prepare tx %v secondary intents, range group %v", t.GetTxId(), len(secIntents))
	if len(secIntents) == 0 {
		return
	}
	doPrepareFunc := func(tx *TxObj, subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
		log.Debug("doPrepareFunc: prepare tx %v secondary intents %v", tx.GetTxId(), intents)
		req := &txnpb.PrepareRequest{
			TxnId:      tx.GetTxId(),
			Local:      tx.IsLocal(),
			Intents:    intents,
			PrimaryKey: tx.GetPrimaryKey(),
			LockTtl:    tx.Timeout,
		}
		e := tx.proxy.handlePrepare(subCtx, req, tx.GetTable())
		if e != nil {
			handleChannel <- &TxnDealHandle{intents: intents, err: e}
			return
		}
		handleChannel <- &TxnDealHandle{intents: intents, err: nil}
	}
	err = t.handleSecondary(ctx, secIntents, doPrepareFunc)
	if err != nil {
		log.Error("[commit]txn %v prepare secondary intents err %v", t.GetTxId(), err)
		return
	}
	log.Debug("prepare tx %v secondary intents success", t.GetTxId())
	return
}

func (t *TxObj) decidePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent,
	secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {

	//todo refactor and abstract to func call about error: ErrRouteChange
	var errForRetry error
	/**
		loop solve: occur ErrRouteChange when decide intents with primary row, regroup intents
	 */
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[commit]%s execute prepare primary intents timeout", ctx)
				return
			}
		}

		if err == dskv.ErrRouteChange {
			var partSecIntents [][]*txnpb.TxnIntent
			priIntents, partSecIntents, err = regroupIntentsByRange(ctx, t.GetTable(), priIntents)
			if err != nil || len(priIntents) == 0 {
				return
			}
			if len(partSecIntents) > 0 {
				secIntents = append(secIntents, partSecIntents...)
			}
		}
		log.Debug("decide tx %v primary intents, intents size: %v", t.GetTxId(), len(priIntents))
		var keys = make([][]byte, len(priIntents))
		for i, intent := range priIntents {
			keys[i] = intent.GetKey()
		}
		req := &txnpb.DecideRequest{
			TxnId:  t.GetTxId(),
			Status: status,
			Keys:   keys,
		}
		var resp *txnpb.DecideResponse
		resp, err = t.proxy.handleDecide(ctx, req, t.GetTable())
		if err != nil {
			if err == dskv.ErrRouteChange {
				continue
			}
			return
		}
		if resp.GetErr() != nil {
			err = convertTxnErr(resp.Err)
			log.Error("handleDecide txn[%v] error ", req.GetTxnId(), err)
			return
		}
		log.Debug("decide txn[%v] primary intents success ", req.GetTxnId())
		return
	}
}

func (t *TxObj) decideSecondaryIntents(secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {
	log.Debug("start to decide tx %v secondary intents, range group %v", t.GetTxId(), len(secIntents))
	ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	if len(secIntents) > 0 {
		doDecideFunc := func(tx *TxObj, subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
			log.Debug("doDecideFunc: decide tx %v secondary intents %v", tx.GetTxId(), intents)
			var keys = make([][]byte, len(intents))
			for i, intent := range intents {
				keys[i] = intent.GetKey()
			}
			var (
				req = &txnpb.DecideRequest{
					TxnId:  tx.GetTxId(),
					Status: status,
					Keys:   keys,
				}
				resp *txnpb.DecideResponse
				e    error
			)

			resp, e = tx.proxy.handleDecide(subCtx, req, tx.GetTable())
			if e != nil {
				handleChannel <- &TxnDealHandle{intents: intents, err: e}
			}
			if resp.Err != nil {
				e = convertTxnErr(resp.Err)
				handleChannel <- &TxnDealHandle{intents: intents, err: e}
				return
			}
			handleChannel <- &TxnDealHandle{intents: intents, err: nil}
		}
		err = t.handleSecondary(ctx, secIntents, doDecideFunc)
		if err != nil {
			log.Error("[commit]txn %v async decide secondary intents err %v", t.GetTxId(), err)
			return
		}
		log.Debug("decide txn[%v] secondary intents success ", t.GetTxId())
	}
	return
}

func (t *TxObj) handleSecondary(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent,
	handleFunc func(*TxObj, *dskv.ReqContext, []*txnpb.TxnIntent, chan *TxnDealHandle)) (err error) {
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
			_, handleIntents, err = regroupIntentsByRange(ctx, t.GetTable(), needRetryIntents)
			if err != nil {
				return
			}
		}
		var (
			handleGroup   = len(handleIntents)
			handleChannel = make(chan *TxnDealHandle, handleGroup)
		)
		for _, group := range handleIntents {
			cClone := ctx.Clone()
			go handleFunc(t, cClone, group, handleChannel)
		}
		for i := 0; i < handleGroup; i++ {
			txnHandle := <-handleChannel
			if txnHandle.err != nil {
				err = txnHandle.err
				if err == dskv.ErrRouteChange {
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

func (t *TxObj) changeTxStatus(newStatus txnpb.TxnStatus) (bool, error) {
	if newStatus != txnpb.TxnStatus_INIT &&
		newStatus != txnpb.TxnStatus_COMMITTED &&
		newStatus != txnpb.TxnStatus_ABORTED {
		return false, getErrTxnStatusNoSupported(newStatus)
	}
	var (
		passed    bool
		err       error
		oldStatus = t.status
	)
	switch oldStatus {
	case txnpb.TxnStatus_INIT:
		passed = true
	case txnpb.TxnStatus_COMMITTED:
		if newStatus == txnpb.TxnStatus_INIT {
			err = getErrTxnStatusConflicted(oldStatus, newStatus)
		} else if newStatus == txnpb.TxnStatus_ABORTED {
			passed = true
		}
	case txnpb.TxnStatus_ABORTED:
		err = getErrTxnStatusConflicted(oldStatus, newStatus)
	default:
		err = getErrTxnStatusNoSupported(oldStatus)
	}
	if err != nil || !passed {
		return passed, err
	}
	t.status = newStatus
	if oldStatus != newStatus {
		log.Info("change transaction[%v] status[%v] to [%v]", t.GetTxId(), oldStatus, newStatus)
	}
	return passed, nil
}

func (t *TxObj) getSecondaryKeys() [][]byte {
	var secondaryKeys = make([][]byte, 0)
	for _, intent := range t.intents {
		if intent.GetIsPrimary() {
			continue
		}
		secondaryKeys = append(secondaryKeys, intent.GetKey())
	}
	return secondaryKeys
}

func isLocalTxn(priIntents []*txnpb.TxnIntent, secIntents [][]*txnpb.TxnIntent) bool {
	if len(priIntents) > 0 && len(secIntents) == 0 {
		return true
	}
	return false
}

func getErrTxnStatusConflicted(oldStatus, newStatus txnpb.TxnStatus) error {
	return fmt.Errorf("tx status[%v] change to [%v] is conflicted", oldStatus, newStatus)
}

func getErrTxnStatusNoSupported(status txnpb.TxnStatus) error {
	return fmt.Errorf("tx status[%v] is not supported", status)
}
