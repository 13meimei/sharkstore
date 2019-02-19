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
	primaryKey []byte

	status txnpb.TxnStatus

	intents []*txnpb.TxnIntent

	//todo init
	proxy *Proxy

	Timeout uint64
}

func NewTx(implicit bool, timeout uint64) TX {
	//todo generate txnId
	tx := &TxObj{txId: "", implicit: implicit}
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

	if err = t.checkTxnIntentLength(); err != nil {
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
	for i, intent := range intents {
		if i == 0 && len(t.GetPrimaryKey()) == 0 {
			intent.IsPrimary = true
			t.primaryKey = intent.GetKey()
		}
		t.intents = append(t.intents, intent)
	}
	if err = t.checkTxnIntentLength(); err != nil {
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
	if err = t.checkTxnIntentLength(); err != nil {
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

//prepare and decide 2PL
func (t *TxObj) Commit() (err error) {
	var passed bool
	passed, err = t.changeTxStatus(txnpb.TxnStatus_COMMITTED)
	if err != nil || !passed {
		return
	}
	if len(t.intents) == 0 {
		return
	}
	ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	var errForRetry error
	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[commit]%s execute timeout", ctx)
				return
			}
		}
		//prepare: first write primary intents, second write secondary intents
		var (
			priIntentsGroup []*txnpb.TxnIntent
			secIntentsGroup [][]*txnpb.TxnIntent
		)
		priIntentsGroup, secIntentsGroup, err = regroupIntentsByRange(ctx, nil, t.intents)
		if err != nil {
			return
		}
		//todo local txn optimize to 1PL
		isLocal := isLocalTxn(priIntentsGroup, secIntentsGroup)

		//prepare primary row intents
		err = t.preparePrimaryIntents(ctx, priIntentsGroup, isLocal)
		if err != nil {
			if err == dskv.ErrRouteChange {
				continue
			}
			return
		}
		if !isLocal {
			//concurrency prepare secondary row intents
			err = t.prepareSecondaryIntents(ctx, secIntentsGroup, isLocal)
			if err != nil {
				if err == dskv.ErrRouteChange {
					continue
				}
				return
			}
		}
		//decide:
		err = t.decidePrimaryIntents(ctx, priIntentsGroup, txnpb.TxnStatus_COMMITTED)
		if err != nil {
			return
		}
		go func() {
			t.asyncDecideSecondaryAndClear(ctx, secIntentsGroup, txnpb.TxnStatus_COMMITTED)
		}()
	}
	return
}

/**
  rollback current transaction because of error, and so on
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
	//ctx := dskv.NewPRConext(int(t.Timeout * 1000))
	//var (
	//	priIntentsGroup []*txnpb.TxnIntent
	//	secIntentsGroup [][]*txnpb.TxnIntent
	//)
	//priIntentsGroup, secIntentsGroup, err = regroupIntentsByRange(ctx, nil, t.intents)
	//if err != nil {
	//	return
	//}
	return
}

/**
  rollback previous expired transaction
 */
func (t *TxObj) recover() {

}

func (t *TxObj) preparePrimaryIntents(ctx *dskv.ReqContext, priIntents []*txnpb.TxnIntent, isLocal bool) (err error) {
	if len(priIntents) == 0 {
		return
	}
	var (
		req = &txnpb.PrepareRequest{
			TxnId:         t.GetTxId(),
			Local:         isLocal,
			Intents:       priIntents,
			PrimaryKey:    t.GetPrimaryKey(),
			LockTtl:       t.Timeout,
			SecondaryKeys: t.getSecondaryKeys(),
		}
		errForRetry error
	)

	for {
		if errForRetry != nil {
			errForRetry = ctx.GetBackOff().Backoff(dskv.BoMSRPC, errForRetry)
			if errForRetry != nil {
				log.Error("[commit]%s execute prepare primary intents timeout", ctx)
				return
			}
		}
		var prepareResp *txnpb.PrepareResponse
		prepareResp, err = t.proxy.handlePrepare(ctx, req, nil)
		if err != nil {
			return err
		}
		if len(prepareResp.Errors) > 0 {
			needClearTxs := make([][]byte, 0)
			for _, txError := range prepareResp.GetErrors() {
				switch txError.GetErrType() {
				case txnpb.TxnError_LOCKED:
					lockErr := txError.GetLockErr()
					if lockErr.GetInfo() != nil && lockErr.GetInfo().GetTimeout() {
						needClearTxs = append(needClearTxs, lockErr.GetKey())
					}
				default:
					err = GetTxnError(txError)
					return

				}
			}

			if len(needClearTxs) > 0 {
				//clearTimeoutTxn(needClearTxs)
			}

		}
	}
	return

}

func (t *TxObj) prepareSecondaryIntents(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent, isLocal bool) (err error) {
	if len(secIntents) == 0 {
		return
	}
	var errChannel = make(chan error, len(secIntents))
	defer close(errChannel)
	for _, group := range secIntents {
		go func(intents []*txnpb.TxnIntent) {
			req := &txnpb.PrepareRequest{
				TxnId:      t.GetTxId(),
				Local:      isLocal,
				Intents:    intents,
				PrimaryKey: t.GetPrimaryKey(),
				LockTtl:    t.Timeout,
			}
			var prepareResp *txnpb.PrepareResponse
			prepareResp, err = t.proxy.handlePrepare(ctx, req, nil)
			if err != nil {
				errChannel <- err
				return
			}
			if len(prepareResp.Errors) > 0 {
				//	for _, respErr := range prepareResp.Errors {
				//		switch respErr.ErrType {
				//		case
				//		}
				//	}
			}
			errChannel <- nil
		}(group)
	}
	for i := 0; i < len(secIntents); i++ {
		if v := <-errChannel; v != nil {
			return v
		}
	}
	return
}

func (t *TxObj) decidePrimaryIntents(ctx *dskv.ReqContext, intents []*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {
	var passed bool
	passed, err = t.changeTxStatus(status)
	if err != nil || !passed {
		return
	}
	var keys = make([][]byte, len(intents))
	for _, intent := range intents {
		keys = append(keys, intent.GetKey())
	}
	req := &txnpb.DecideRequest{
		TxnId:  t.GetTxId(),
		Status: status,
		Keys:   keys,
	}
	t.proxy.handleDecide(ctx, req, nil)
	return
}

func (t *TxObj) asyncDecideSecondaryAndClear(ctx *dskv.ReqContext, secIntents [][]*txnpb.TxnIntent, status txnpb.TxnStatus) (err error) {
	if len(secIntents) == 0 {
		return
	}
	var errChannel = make(chan error, len(secIntents))
	for _, group := range secIntents {
		go func(intents []*txnpb.TxnIntent) {
			//var keys = make([][]byte, len(intents))
			//for _, intent := range intents {
			//	keys = append(keys, intent.GetKey())
			//}
			//req := &txnpb.DecideRequest{
			//	TxnId:  t.GetTxId(),
			//	Status: status,
			//	Keys:   keys,
			//}
			//var decideResp *txnpb.DecideResponse
			//decideResp, err = t.proxy.handleDecide(ctx, req, nil)
			//if err != nil {
			//	errChannel <- err
			//	return
			//}
			errChannel <- nil
		}(group)
	}
	for i := 0; i < len(secIntents); i++ {
		if v := <-errChannel; v != nil {
			return v
		}
	}
	t.proxy.handleCleanup(ctx, t.GetTxId(), t.GetPrimaryKey(), nil)
	return
}

func (t *TxObj) checkTxnIntentLength() error {
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

func GetTxnError(txError *txnpb.TxnError) error{
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