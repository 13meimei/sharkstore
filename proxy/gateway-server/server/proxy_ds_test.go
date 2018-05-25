package server

//import (
//	"model/pkg/metapb"
//	"testing"
//)

//func TestProxyNewDs(t *testing.T) {
//	columns := []*columnInfo{
//		&columnInfo{name: "id", typ: metapb.DataType_BigInt, isUnsigned: true, isPK: true},
//		&columnInfo{name: "name", typ: metapb.DataType_Varchar},
//		&columnInfo{name: "balance", typ: metapb.DataType_Double},
//	}
//	p := newDsTestProxy(columns, nil)
//	defer p.Close()
//	// testProxySelect(t, p, [][]string{}, "select * from "+testTableName+" where id > 1 and id < 100")
//
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(1, 'myname', 0.0075)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(id,name,balance) values(2, 'myname2', 1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(3, 'myname3', 3.1)")
//	testProxyInsert(t, p, 1, "insert into "+testTableName+"(iD,nAMe,bAlaNce) values(4, 'myname4', NULL)")
//
//	// select and check
//	expected := [][]string{
//		[]string{"1", "myname", "0.0075"},
//		[]string{"2", "myname2", "1"},
//		[]string{"3", "myname3", "3.1"},
//		[]string{"4", "myname4", "NULL"},
//	}
//	testProxySelect(t, p, expected[1:], "select * from "+testTableName+" where id > 1")
//	testProxySelect(t, p, expected[1:], "select * from "+testTableName+" where id >= 2")
//	testProxySelect(t, p, expected[0:1], "select * from "+testTableName+" where id = 1")
//	testProxySelect(t, p, expected[:2], "select * from "+testTableName+" where id < 3")
//	testProxySelect(t, p, expected[:3], "select * from "+testTableName+" where id <= 3")
//}
