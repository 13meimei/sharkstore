package server

import (
	"fmt"
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

	proxy *Proxy

	Timeout uint64
}

func NewTx(implicit bool, p *Proxy, timeout uint64) TX {
	//todo generate txnId
	tx := &TxObj{txId: "", proxy: p, implicit: implicit}
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

	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			t.Rollback()
			return
		}
	}
	return nil
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
	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			t.Rollback()
			return
		}
	}
	return nil
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

	if t.IsImplicit() {
		if err = t.Commit(); err != nil {
			t.Rollback()
			return
		}
	}
	return nil
}

/**
	commit:  prepare and decide 2PL duration tx timeout (if tx is local, optimize to 1PL)
	prepare: first write primary intents, second write secondary intents
	decide:  decide primary intents, then async to decide secondary intents, last clear tx record
 */
func (t *TxObj) Commit() (err error) {
	var passed bool
	passed, err = t.changeTxStatus(txnpb.TxnStatus_COMMITTED)
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
	priIntentsGroup, secIntentsGroup, err = regroupIntentsByRange(ctx, nil, t.intents)
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
		return
	}
	//todo local txn optimize to 1PL
	//if !t.IsLocal() {
	//concurrency prepare secondary row intents
	err = t.prepareSecondaryIntents(ctx, secIntentsGroup)
	if err != nil {
		return
	}

	//decide:
	err = t.decidePrimaryIntents(ctx, priIntentsGroup, secIntentsGroup, txnpb.TxnStatus_COMMITTED)
	if err != nil {
		return
	}
	//async call
	//todo goroutine num control
	go func(intents [][]*txnpb.TxnIntent) {
		var e error
		if e = t.decideSecondaryIntents(intents, txnpb.TxnStatus_COMMITTED); e != nil {
			log.Warn("async decide txn %v secondary intent error %v", t.GetTxId(), e)
			return
		}
		if e = t.proxy.handleCleanup(ctx, t.GetTxId(), t.GetPrimaryKey(), nil); e != nil {
			log.Warn("async decide txn %v secondary intent error %v", t.GetTxId(), e)
		}
		return
	}(secIntentsGroup)
	//}
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
	var (
		status = txnpb.TxnStatus_ABORTED
		secondaryKeys [][]byte
		ctx = dskv.NewPRConext(int(t.Timeout * 1000))
	)
	//first try to decide expired tx to aborted status
	secondaryKeys, err = t.proxy.tryRollbackTxn(ctx, t.GetTxId(), &status, t.GetPrimaryKey(), true)
	if err != nil {
		return
	}
	if status == txnpb.TxnStatus_COMMITTED {
		log.Error("rollback txn error, because ds let commit")
	}
	//decide all secondary keys
	if err = t.proxy.decideSecondaryKeys(ctx,t.GetTxId(), status, secondaryKeys, nil); err != nil {
		return
	}
	//decide primary key
	if err = t.proxy.decidePrimaryKey(ctx,t.GetTxId(), status, t.GetPrimaryKey(), nil); err != nil {
		return
	}
	//clear up
	return t.proxy.handleCleanup(ctx, t.GetTxId(), t.GetPrimaryKey(), nil)
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
			var partSecIntents = make([][]*txnpb.TxnIntent, 0)
			priIntents, partSecIntents, err = regroupIntentsByRange(ctx, nil, priIntents)
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

		err = t.proxy.handlePrepare(ctx, req, nil)
		if err != nil {
			if err == dskv.ErrRouteChange {
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
	if len(secIntents) == 0 {
		return
	}
	doPrepareFunc := func(subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
		req := &txnpb.PrepareRequest{
			TxnId:      t.GetTxId(),
			Local:      t.IsLocal(),
			Intents:    intents,
			PrimaryKey: t.GetPrimaryKey(),
			LockTtl:    t.Timeout,
		}
		err = t.proxy.handlePrepare(subCtx, req, nil)
		if err != nil {
			handleChannel <- &TxnDealHandle{intents: intents, err: err}
			return
		}
		handleChannel <- &TxnDealHandle{intents: intents, err: nil}
	}
	err = handleSecondary(ctx, secIntents, doPrepareFunc)
	if err != nil {
		log.Error("[commit]txn %v prepare secondary intents err %v", t.GetTxId(), err)
	}
	return
}

func (t *TxObj) decidePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent,
	secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {

	var passed bool
	passed, err = t.changeTxStatus(status)
	if err != nil || !passed {
		return
	}

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
			priIntents, partSecIntents, err = regroupIntentsByRange(ctx, nil, priIntents)
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
		resp, err = t.proxy.handleDecide(ctx, req, nil)
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
		doDecideFunc := func(subCtx *dskv.ReqContext, intents []*txnpb.TxnIntent, handleChannel chan *TxnDealHandle) {
			var keys = make([][]byte, len(intents))
			for _, intent := range intents {
				keys = append(keys, intent.GetKey())
			}
			req := &txnpb.DecideRequest{
				TxnId:  t.GetTxId(),
				Status: status,
				Keys:   keys,
			}
			var resp *txnpb.DecideResponse
			resp, err = t.proxy.handleDecide(ctx, req, nil)
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
		err = handleSecondary(ctx, secIntents, doDecideFunc)
		if err != nil {
			log.Error("[commit]txn %v async decide secondary intents err %v", t.GetTxId(), err)
			return
		}
	}
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
