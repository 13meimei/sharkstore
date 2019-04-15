package server

import (
	"fmt"
	"bytes"
	"util"
	"util/log"
	"model/pkg/metapb"
	"model/pkg/kvrpcpb"
)

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
			indexValue []byte
			kv         *kvrpcpb.KeyValue
		)
		colIndex, ok := colMap[col.GetName()]
		if !ok {
			indexValue = initValueByDataType(col)
		} else {
			indexValue = rowValue[colIndex]
		}
		kv, err = encodeNonUniqueIndexKv(t, colMap, col, indexValue, rowValue)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, kv)
	}
	return keyValues, nil
}

func encodeNonUniqueIndexKv(t *Table, colMap map[string]int, idxCol *metapb.Column, idxVal []byte, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	key, err := encodeUniqueIndexKey(t, idxCol, idxVal)
	if err != nil {
		return nil, err
	}

	key, err = encodePrimaryKeys(t, key, colMap, rowValue)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.KeyValue{
		Key: key,
	}, nil
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
			indexValue []byte
			kv         *kvrpcpb.KeyValue
		)
		colIndex, ok := colMap[col.GetName()]
		if !ok {
			indexValue = initValueByDataType(col)
		} else {
			indexValue = rowValue[colIndex]
		}
		kv, err = encodeUniqueIndexKV(t, colMap, col, indexValue, rowValue)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, kv)
	}
	return keyValues, nil
}

func encodeUniqueIndexKV(t *Table, colMap map[string]int, idxCol *metapb.Column, idxVal []byte, rowValue InsertRowValue) (*kvrpcpb.KeyValue, error) {
	key, err := encodeUniqueIndexKey(t, idxCol, idxVal)
	if err != nil {
		return nil, err
	}
	var value []byte
	value, err = encodePrimaryKeys(t, value, colMap, rowValue)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.KeyValue{
		Key:   key,
		Value: value,
	}, nil
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

// EncodeIndexes: encode unique and non-unique index data
func (p *Proxy) EncodeIndexesForUpd(t *Table, colMap map[string]int, oldRowValue, newRowValue InsertRowValue) ([]*kvrpcpb.KeyValue, []*kvrpcpb.KeyValue, error) {
	var (
		indexKvPairsForInsert, indexKvPairForDel []*kvrpcpb.KeyValue
		err                                      error
	)
	uniqueIndexCols := t.AllUniqueIndexes()
	nonUniqueIndexCols := t.AllNonUniqueIndexes()

	if len(uniqueIndexCols) > 0 {
		for _, col := range uniqueIndexCols {
			var (
				oldIdxVal, newIdxVal []byte
				delKv, insertKv      *kvrpcpb.KeyValue
			)
			colIndex, ok := colMap[col.GetName()]
			if !ok {
				err = fmt.Errorf("")
				return nil, nil, err
			}
			oldIdxVal = oldRowValue[colIndex]
			newIdxVal = newRowValue[colIndex]
			if bytes.Compare(oldIdxVal, newIdxVal) == 0 {
				continue
			}

			delKv, err = encodeUniqueIndexKV(t, colMap, col, oldIdxVal, oldRowValue)
			if err != nil {
				return nil, nil, err
			}
			indexKvPairForDel = append(indexKvPairForDel, delKv)

			insertKv, err = encodeUniqueIndexKV(t, colMap, col, newIdxVal, newRowValue)
			if err != nil {
				return nil, nil, err
			}
			indexKvPairsForInsert = append(indexKvPairsForInsert, insertKv)
		}
	}

	if len(nonUniqueIndexCols) > 0 {
		for _, col := range nonUniqueIndexCols {
			var (
				oldIdxVal, newIdxVal []byte
				delKv, insertKv      *kvrpcpb.KeyValue
			)
			colIndex, ok := colMap[col.GetName()]
			if !ok {
				err = fmt.Errorf("")
				return nil, nil, err
			}
			oldIdxVal = oldRowValue[colIndex]
			newIdxVal = newRowValue[colIndex]
			if bytes.Compare(oldIdxVal, newIdxVal) == 0 {
				continue
			}

			delKv, err = encodeNonUniqueIndexKv(t, colMap, col, oldIdxVal, oldRowValue)
			if err != nil {
				return nil, nil, err
			}
			indexKvPairForDel = append(indexKvPairForDel, delKv)

			insertKv, err = encodeNonUniqueIndexKv(t, colMap, col, newIdxVal, newRowValue)
			if err != nil {
				return nil, nil, err
			}
			indexKvPairsForInsert = append(indexKvPairsForInsert, insertKv)
		}
	}
	return indexKvPairForDel, indexKvPairsForInsert, nil
}
