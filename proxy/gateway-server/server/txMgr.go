package server

import (
	"fmt"
	"util"
	"util/log"
	"model/pkg/txn"
	"proxy/store/dskv"
)

const (
	TXN_INTENT_MAX_LENGTH        = 100
	TXN_DEFAULT_TIMEOUT   uint64 = 50
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
	if err = t.checkTxIntentLength(); err != nil {
		return
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
		log.Info("tx %v commit implicitly success", t.GetTxId())
	}
	return
}

func (t *TxObj) Update(intents []*txnpb.TxnIntent) (err error) {
	if len(intents) == 0 {
		log.Info("[txn]update intent is empty")
		return
	}
	//todo merge current tx data
	for i, intent := range intents {
		if i == 0 && len(t.GetPrimaryKey()) == 0 {
			intent.IsPrimary = true
			t.primaryKey = intent.GetKey()
		}
		t.intents = append(t.intents, intent)
	}
	if err = t.checkTxIntentLength(); err != nil {
		return
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
		log.Info("tx %v commit implicitly success", t.GetTxId())
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
	if err = t.checkTxIntentLength(); err != nil {
		return
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
		log.Info("tx %v commit implicitly success", t.GetTxId())
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
		priIntentsGroup []*txnpb.TxnIntent
		secIntentsGroup [][]*txnpb.TxnIntent
	)
	priIntentsGroup, secIntentsGroup, err = regroupIntentsByRange(ctx, t.GetTable(), t.intents)
	if err != nil {
		return
	}
	/**
	  func: prepare primary row intents
	  priIntentsGroup and secIntentsGroup size are affected by priIntentsGroup prepare result,
	  so they can be changed at func 'preparePrimaryIntents'(reference)
	 */
	err = t.preparePrimaryIntents(ctx, priIntentsGroup, secIntentsGroup)
	if err != nil {
		log.Error("prepare tx %v primary intents error %v", t.GetTxId(), err)
		return
	}
	//todo local txn optimize to 1PL
	//if !t.IsLocal() {
	//concurrency prepare secondary row intents
	err = t.prepareSecondaryIntents(ctx, secIntentsGroup)
	if err != nil {
		log.Error("prepare tx %v secondary intents error %v", t.GetTxId(), err)
		return
	}

	//decide:
	err = t.decidePrimaryIntents(ctx, priIntentsGroup, secIntentsGroup, status)
	if err != nil {
		log.Error("decide tx %v primary intents error %v", t.GetTxId(), err)
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
	err = t.proxy.handleRecoverPrimary(ctx, t.GetTxId(), t.GetPrimaryKey(), nil, true, t.GetTable())
	if err != nil {
		log.Info("txn %v rollback err %v", t.GetTxId(), err)
		return
	}
	log.Info("txn %v rollback success", t.GetTxId())
	return
}

func (t *TxObj) preparePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent, secIntents [][]*txnpb.TxnIntent) (err error) {
	log.Info("start to prepare tx %v primary intents", t.GetTxId())
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
			var partSecIntents = make([][]*txnpb.TxnIntent, 0)
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
		return
	}
}

func (t *TxObj) prepareSecondaryIntents(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent) (err error) {
	log.Info("start to prepare tx %v secondary intents, intents size %v", t.GetTxId(), len(secIntents))
	if len(secIntents) == 0 {
		return
	}
	doPrepareFunc := func(tx *TxObj, subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
		log.Info("prepare tx %v secondary intents %v", tx.GetTxId(), intents)
		req := &txnpb.PrepareRequest{
			TxnId:      tx.GetTxId(),
			Local:      tx.IsLocal(),
			Intents:    intents,
			PrimaryKey: tx.GetPrimaryKey(),
			LockTtl:    tx.Timeout,
		}
		err = tx.proxy.handlePrepare(subCtx, req, tx.GetTable())
		if err != nil {
			handleChannel <- &TxnDealHandle{intents: intents, err: err}
			return
		}
		handleChannel <- &TxnDealHandle{intents: intents, err: nil}
	}
	err = t.handleSecondary(ctx, secIntents, doPrepareFunc)
	if err != nil {
		log.Error("[commit]txn %v prepare secondary intents err %v", t.GetTxId(), err)
	}
	return
}

func (t *TxObj) decidePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent,
	secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {

	log.Info("decide tx %v primary intents", t.GetTxId())

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
			var partSecIntents = make([][]*txnpb.TxnIntent, 0)
			priIntents, partSecIntents, err = regroupIntentsByRange(ctx, t.GetTable(), priIntents)
			if err != nil || len(priIntents) == 0 {
				return
			}
			if len(partSecIntents) > 0 {
				secIntents = append(secIntents, partSecIntents...)
			}
		}

		var keys = make([][]byte, len(priIntents))
		for _, intent := range priIntents {
			keys = append(keys, intent.GetKey())
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
		if resp.Err != nil {
			err = convertTxnErr(resp.Err)
			log.Error("handlePrepare txn[%v] error ", req.GetTxnId(), err)
		}
		return
	}
}

func (t *TxObj) decideSecondaryIntents(secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {
	ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	if len(secIntents) > 0 {
		doDecideFunc := func(tx *TxObj, subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
			var keys = make([][]byte, len(intents))
			for _, intent := range intents {
				keys = append(keys, intent.GetKey())
			}
			req := &txnpb.DecideRequest{
				TxnId:  tx.GetTxId(),
				Status: status,
				Keys:   keys,
			}
			var resp *txnpb.DecideResponse
			resp, err = tx.proxy.handleDecide(subCtx, req, tx.GetTable())
			if err != nil {
				handleChannel <- &TxnDealHandle{intents: intents, err: err}

			}
			if resp.Err != nil {
				err = convertTxnErr(resp.Err)
				handleChannel <- &TxnDealHandle{intents: intents, err: err}
				return
			}
			handleChannel <- &TxnDealHandle{intents: intents, err: nil}
		}
		err = t.handleSecondary(ctx, secIntents, doDecideFunc)
		if err != nil {
			log.Error("[commit]txn %v async decide secondary intents err %v", t.GetTxId(), err)
			return
		}
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
		handleGroup := len(handleIntents)
		var handleChannel = make(chan *TxnDealHandle, handleGroup)
		for _, group := range handleIntents {
			cClone := ctx.Clone()
			go handleFunc(t, cClone, group, handleChannel)
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

func (t *TxObj) checkTxIntentLength() error {
	length := len(t.getTxIntents())
	if length > TXN_INTENT_MAX_LENGTH {
		return fmt.Errorf("tx length [%v] is exceed max limit [%v]", length, TXN_INTENT_MAX_LENGTH)
	}
	return nil
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
			log.Warn("locate key failed, err %v", err)
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
		//// 每100个kv切割一下
		//if len(group) >= 100 {
		//	if l.Region.Id == pkRangeId {
		//		priIntentsGroup = group
		//	} else {
		//		secIntentsGroup = append(secIntentsGroup, group)
		//	}
		//	delete(ggroup, l.Region.Id)
		//}
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

func (t *TxObj) getSecondaryKeys() [][]byte {
	if len(t.intents) == 1 {
		return nil
	}
	var secondaryKeys [][]byte
	secondaryKeys = make([][]byte, len(t.intents)-1)
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
