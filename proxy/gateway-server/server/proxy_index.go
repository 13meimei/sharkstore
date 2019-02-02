package server

import (
	"util"
	"util/log"
	"model/pkg/metapb"
	"model/pkg/kvrpcpb"
	"proxy/store/dskv"
)

// insert unique index key, 如果存在，则报错
func (p *Proxy) insertIndexes(context *dskv.ReqContext, t *Table, indexKvPairs []*kvrpcpb.KeyValue) (err error) {
	if len(indexKvPairs) == 0 {
		return
	}
	_, _, err = p.insertRowsWithContext(context, t, indexKvPairs)
	if err != nil {
		log.Error("insert indexes to table [%v:%v] error", t.GetDbId(), t.GetId())
		return
	}
	return nil
}

// delete index key
func (p *Proxy) deleteIndexes(context *dskv.ReqContext, t *Table, indexKeys [][]byte) (err error) {
	if len(indexKeys) == 0 {
		return
	}
	//todo

	return nil
}

// check unique index key, 如果存在，则报错
func (p *Proxy) checkUniqueIndexes(context *dskv.ReqContext, t *Table, uniqueKvPairs []*kvrpcpb.KeyValue) bool {
	var existUniqIndex bool
	//todo
	//for _, kvPair := range uniqueKvPairs {
	//
	//}
	return existUniqIndex
}

// EncodeIndexes: encode unique and non-unique index data
func (p *Proxy) EncodeIndexes(t *Table, colMap map[string]int, rowValue InsertRowValue) ([]*kvrpcpb.KeyValue, error) {
	var (
		indexKvPairs []*kvrpcpb.KeyValue
		err          error
	)
	uniqueIndexCols := t.AllUniqueIndexes()
	nonUniqueIndexCols := t.AllNonUniqueIndexes()

	indexKvPairs, err = p.encodeUniqueIndexRows(t, uniqueIndexCols, colMap, rowValue)
	if err != nil {
		return nil, err
	}
	var indexKvPairs2 []*kvrpcpb.KeyValue
	indexKvPairs2, err = p.encodeNonUniqueIndexRows(t, nonUniqueIndexCols, colMap, rowValue)
	if err != nil {
		return nil, err
	}
	indexKvPairs = append(indexKvPairs, indexKvPairs2...)
	return indexKvPairs, nil
}

// Format of Non-Unique Index Storage Structure:
//  +---------------------------------------------------------------+----------+
//  |                             Key                               |  Value   |
//  +---------------------------------------------------------------+----------+
//  | Store_Prefix_INDEX + tableId + indexId + indexValue + PKValue |(version) |
//  +---------------------------------------------------------------+----------+
//
// version: proxy don't encode the parameter

//encodeNonUniqueIndexRows: encode non-unique index rows
func (p *Proxy) encodeNonUniqueIndexRows(t *Table, indexCols []*metapb.Column, colMap map[string]int, rowValue InsertRowValue) ([]*kvrpcpb.KeyValue, error) {
	keyValues := make([]*kvrpcpb.KeyValue, 0)
	var err error

	for _, col := range indexCols {
		var (
			key, indexValue, value []byte
		)
		colIndex, ok := colMap[col.GetName()]
		if !ok {
			indexValue = initValueByDataType(col)
		} else {
			indexValue = rowValue[colIndex]
		}

		key, err = encodeUniqueIndexKey(t, col, indexValue)
		if err != nil {
			return nil, err
		}

		key, err = encodePrimaryKeys(t, key, colMap, rowValue)
		if err != nil {
			return nil, err
		}

		keyValues = append(keyValues, &kvrpcpb.KeyValue{
			Key:   key,
			Value: value,
		})
	}
	return keyValues, nil
}

// Format of Unique Index Storage Structure:
//  +-----------------------------------------------------+--------------------+
//  |                             Key                     |        Value       |
//  +--------------------------------------------------------------------------+
//  | Store_Prefix_INDEX + tableId + indexId + indexValue | PKValue(+ version) |
//  +-----------------------------------------------------+--------------------+
// version: proxy don't encode the parameter

//encodeUniqueIndexRows: encode unique index rows
func (p *Proxy) encodeUniqueIndexRows(t *Table, indexCols []*metapb.Column, colMap map[string]int, rowValue InsertRowValue) ([]*kvrpcpb.KeyValue, error) {
	if len(indexCols) == 0 {
		return nil, nil
	}
	keyValues := make([]*kvrpcpb.KeyValue, 0)
	var err error

	for _, col := range indexCols {
		var (
			key, indexValue, value []byte
		)
		colIndex, ok := colMap[col.GetName()]
		if !ok {
			indexValue = initValueByDataType(col)
		} else {
			indexValue = rowValue[colIndex]
		}

		key, err = encodeUniqueIndexKey(t, col, indexValue)
		if err != nil {
			return nil, err
		}

		value, err = encodePrimaryKeys(t, value, colMap, rowValue)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, &kvrpcpb.KeyValue{
			Key:   key,
			Value: value,
		})
	}
	return keyValues, nil
}

func encodeUniqueIndexKey(t *Table, col *metapb.Column, idxDefVal []byte) ([]byte, error) {
	var err error
	key := util.EncodeStorePrefix(util.Store_Prefix_INDEX, t.GetId())
	// encode: index column id + index column value
	key, err = util.EncodeIndexKey(key, col, idxDefVal)
	if err != nil {
		log.Error("encode index column[%v] value[%v] error: %v", col.GetName(), idxDefVal, err)
		return nil, err
	}
	return key, nil
}

func initValueByDataType(col *metapb.Column) []byte {
	var value []byte
	//switch col.GetDataType() {
	//case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
	//	if col.GetUnsigned() { // 无符号
	//
	//	} else { // 有符号
	//	}
	//case metapb.DataType_Float, metapb.DataType_Double:
	//
	//case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
	//}
	return value
}
