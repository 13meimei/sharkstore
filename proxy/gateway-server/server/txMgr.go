package server

import (
	"util/log"
	"model/pkg/txn"
	"proxy/store/dskv"
	"master-server/engine/errors"
)

const (
	TXN_INTENT_MAX_LENGTH = 50
	TXN_DEFAULT_TIMEOUT  uint64 = 30
)

type TX interface {
	GetTxId() string
	IsLocal() bool
	Insert([]*txnpb.TxnIntent) error
	Update([]*txnpb.TxnIntent) error
	Select([]*txnpb.TxnIntent) error
	Delete([]*txnpb.TxnIntent) error
	Commit() error
	Rollback() error
}

type TxObj struct {
	txId  string
	local bool
	//todo

	status  txnpb.TxnStatus
	intents []*txnpb.TxnIntent

	proxy *Proxy

	Timeout uint64
}

func NewTx(local bool, timeout uint64) TX {
	//todo generate txnId
	tx := &TxObj{txId: "", local: local}
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

func (t *TxObj) IsLocal() bool {
	if t != nil {
		return t.local
	}
	return false
}

func (t *TxObj) Insert(intents []*txnpb.TxnIntent) (err error) {
	if len(intents) == 0 {
		log.Info("[txn]insert intent is empty")
		return
	}
	for _, intent := range intents {
		t.intents = append(t.intents, intent)
	}
	if len(t.intents) > TXN_INTENT_MAX_LENGTH {
		err = errors.New("")
		return
	}

	if t.IsLocal() {
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
	for _, intent := range intents {
		t.intents = append(t.intents, intent)
	}
	if len(t.intents) > TXN_INTENT_MAX_LENGTH {
		err = errors.New("")
		return
	}

	if t.IsLocal() {
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
	for _, intent := range intents {
		t.intents = append(t.intents, intent)
	}
	if len(t.intents) > TXN_INTENT_MAX_LENGTH {
		err = errors.New("")
		return
	}

	if t.IsLocal() {
		if err = t.Commit(); err != nil {
			t.Rollback()
			return
		}
	}
	return nil
}


//prepare and decide
func (t *TxObj) Commit() (err error) {
	if len(t.intents) == 0 {
		return
	}
	var (
		priIntentsGroup []*txnpb.TxnIntent
		secIntentsGroup [][]*txnpb.TxnIntent
	)
	priIntentsGroup, secIntentsGroup, err = regroupIntentsByRange(nil, nil, t.intents)
	if err != nil {
		return
	}
	t.prepareIntents(priIntentsGroup, secIntentsGroup)
	return
}

func (t *TxObj) prepareIntents(priIntents []*txnpb.TxnIntent, secIntents [][]*txnpb.TxnIntent) {
	//prepare primary row intents
	preparePrimaryIntents(priIntents)
	//concurrency preparpe secondary row intents
	prepareSecondaryIntents(secIntents)
}

func (t *TxObj) decideIntents() {

}

func (t *TxObj) Rollback() (err error) {
	return
}

func writeIntentsByRange(intents []*txnpb.TxnIntent) {

}

func preparePrimaryIntents(priIntents []*txnpb.TxnIntent) {

}

func prepareSecondaryIntents(secIntents [][]*txnpb.TxnIntent) {

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
		// 每100个kv切割一下
		if len(group) >= 100 {
			if l.Region.Id == pkRangeId {
				priIntentsGroup = group
			} else {
				secIntentsGroup = append(secIntentsGroup, group)
			}
			delete(ggroup, l.Region.Id)
		}
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
