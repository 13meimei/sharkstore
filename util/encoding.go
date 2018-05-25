package util

import (
	"fmt"
	"strconv"
	"errors"

	"model/pkg/metapb"
	"util/encoding"
	"util/hack"
	"util/log"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
)

const (
	Store_Prefix_Invalid     uint8 = 0
	Store_Prefix_KV          uint8 = 1
	Store_Prefix_Range       uint8 = 2
	Store_Prefix_RaftLog     uint8 = 3
)

func Encode(v proto.Message) ([]byte, error) {
	return proto.Marshal(v)
}

func Decode(buf []byte, v proto.Message) error {
	return proto.Unmarshal(buf, v)
}

func EncodeStorePrefix(prefix uint8, table_id uint64) ([]byte){
	var buff []byte
	buff = make([]byte, 9)
	buff[0] = byte(prefix)
	binary.BigEndian.PutUint64(buff[1:], table_id)
	return buff
}

func DecodeStorePrefix(buff []byte) (prefix uint8, table_id uint64, err error) {
	if len(buff) < 9 {
		err = errors.New("invalid param")
		return
	}
	prefix = uint8(buff[0])
	table_id = binary.BigEndian.Uint64(buff[1:])
	return
}


// EncodeColumnValue 编码列 先列ID再列值
// Note: 编码后不保持排序属性（即如果a > b, 那么编码后的字节数组 bytes.Compare(encA, encB) >0 不一定成立)
func EncodeColumnValue(buf []byte, col *metapb.Column, sval []byte) ([]byte, error) {
	log.Debug("---column %v: %v", col.GetName(), sval)
	if len(sval) == 0 {
		return buf, nil
	}
	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		if col.Unsigned { // 无符号
			ival, err := strconv.ParseUint(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse unsigned integer failed(%s) when encoding column(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeIntValue(buf, uint32(col.Id), int64(ival)), nil
		} else { // 有符号
			ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse integer failed(%s) when encoding column(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeIntValue(buf, uint32(col.Id), ival), nil
		}
	case metapb.DataType_Float, metapb.DataType_Double:
		fval, err := strconv.ParseFloat(hack.String(sval), 64)
		if err != nil {
			return nil, fmt.Errorf("parse float failed(%s) when encoding column(%s)", err.Error(), col.Name)
		}
		return encoding.EncodeFloatValue(buf, uint32(col.Id), fval), nil
	case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
		return encoding.EncodeBytesValue(buf, uint32(col.Id), sval), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s) when encoding column(%s)", col.DataType.String(), col.Name)
	}
}

// DecodeColumnValue 解码列
func DecodeColumnValue(buf []byte, col *metapb.Column) ([]byte, interface{}, error) {
	// check Null
	_, _, _, typ, err := encoding.DecodeValueTag(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("decode value tag for column(%v) failed(%v)", col.Name, err)
	}
	if typ == encoding.Null {
		_, length, err := encoding.PeekValueLength(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("decode null value length for column(%v) failed(%v)", col.Name, err)
		}
		return buf[length:], nil, nil
	}

	// // 列ID是否一致
	// if colID != uint32(col.Id) {
	// 	return nil, nil, fmt.Errorf("mismatch column id for column(%v): %d != %d", col.Name, colID, col.Id)
	// }

	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		remainBuf, ival, err := encoding.DecodeIntValue(buf)
		if col.Unsigned {
			return remainBuf, uint64(ival), err
		} else {
			return remainBuf, ival, err
		}
	case metapb.DataType_Float, metapb.DataType_Double:
		return encoding.DecodeFloatValue(buf)
	case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
		return encoding.DecodeBytesValue(buf)
	default:
		return nil, nil, fmt.Errorf("unsupported type(%s) when decoding column(%s)", col.DataType.String(), col.Name)
	}
}

// EncodePrimaryKey 编码主键列 不编码列ID 保持排序属性
func EncodePrimaryKey(buf []byte, col *metapb.Column, sval []byte) ([]byte, error) {
	switch col.DataType {
	case metapb.DataType_Tinyint, metapb.DataType_Smallint, metapb.DataType_Int, metapb.DataType_BigInt:
		if col.Unsigned { // 无符号整型
			ival, err := strconv.ParseUint(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse unsigned integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeUvarintAscending(buf, ival), nil
		} else { // 有符号整型
			ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
			}
			return encoding.EncodeVarintAscending(buf, ival), nil
		}
	case metapb.DataType_Float, metapb.DataType_Double:
		fval, err := strconv.ParseFloat(hack.String(sval), 64)
		if err != nil {
			return nil, fmt.Errorf("parse float failed(%s) when encoding pk(%s)", err.Error(), col.Name)
		}
		return encoding.EncodeFloatAscending(buf, fval), nil
	case metapb.DataType_Varchar, metapb.DataType_Binary, metapb.DataType_Date, metapb.DataType_TimeStamp:
		return encoding.EncodeBytesAscending(buf, sval), nil
	default:
		return nil, fmt.Errorf("unsupported type(%s) when encoding pk(%s)", col.DataType.String(), col.Name)
	}
}
