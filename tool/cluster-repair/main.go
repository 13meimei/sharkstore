package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/boltdb/bolt"

	"model/pkg/metapb"
	"util"
)

var dbFile = "/Users/gaojianlong/Downloads/fbase.db"

var dbBucket = []byte("DbBucket")
var dbPrefix = []byte("schema db ")
var tablePrefix = []byte("schema table ")
var rangePrefix = []byte("schema range ")
var nodePrefix = []byte("schema node ")
var deleteRangePrefix = []byte("schema deleted_range ")
var incrIDKey = []byte("$auto_increment_id")

func getDBs(db *bolt.DB) (dbs []*metapb.DataBase) {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		c := b.Cursor()

		for k, v := c.Seek(dbPrefix); k != nil && bytes.HasPrefix(k, dbPrefix); k, v = c.Next() {
			var database metapb.DataBase
			err := database.Unmarshal(v)
			if err != nil {
				panic(err)
			}
			dbs = append(dbs, &database)
			fmt.Printf("find databases %s, id: %d\n", database.GetName(), database.GetId())
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return dbs
}

func getTables(db *bolt.DB) []*metapb.Table {
	var tables []*metapb.Table
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		c := b.Cursor()

		for k, v := c.Seek(tablePrefix); k != nil && bytes.HasPrefix(k, tablePrefix); k, v = c.Next() {
			var t metapb.Table
			err := t.Unmarshal(v)
			if err != nil {
				panic(err)
			}
			tables = append(tables, &t)
			fmt.Printf("find table %s.%s, id: %d\n", t.GetDbName(), t.GetName(), t.GetId())
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return tables
}

func getTableRanges(db *bolt.DB, tableID uint64) (ranges []*metapb.Range) {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		c := b.Cursor()

		for k, v := c.Seek(rangePrefix); k != nil && bytes.HasPrefix(k, rangePrefix); k, v = c.Next() {
			var r metapb.Range
			err := r.Unmarshal(v)
			if err != nil {
				panic(err)
			}
			if r.GetTableId() == tableID {
				ranges = append(ranges, &r)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

func getNodes(db *bolt.DB) (nodes []*metapb.Node) {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		c := b.Cursor()

		for k, v := c.Seek(nodePrefix); k != nil && bytes.HasPrefix(k, nodePrefix); k, v = c.Next() {
			var n metapb.Node
			err := n.Unmarshal(v)
			if err != nil {
				panic(err)
			}
			nodes = append(nodes, &n)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

func getNodeSelector(db *bolt.DB) func() uint64 {
	var nodeIDs []uint64
	nodes := getNodes(db)
	for _, n := range nodes {
		if n.GetState() == metapb.NodeState_N_Login {
			nodeIDs = append(nodeIDs, n.GetId())
		}
	}
	if len(nodeIDs) == 0 {
		panic("insufficent login nodes")
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func() uint64 {
		return nodeIDs[rnd.Int()%len(nodeIDs)]
	}
}

var autoIncrID uint64

func getIDGenerator(db *bolt.DB) func() uint64 {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		value := b.Get(incrIDKey)
		if len(value) == 0 {
			autoIncrID = 0
		} else if len(value) == 8 {
			autoIncrID = binary.BigEndian.Uint64(value)
			fmt.Printf("max incr id: %d\n", autoIncrID)
		} else {
			panic(fmt.Sprintf("invalid auto incr value: %v", value))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return func() uint64 {
		autoIncrID++
		return autoIncrID
	}
}

func tableKeyScope(t *metapb.Table) (start []byte, limit []byte) {
	return util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId()),
		util.EncodeStorePrefix(util.Store_Prefix_KV, t.GetId()+1)
}

func tablePKs(t *metapb.Table) []*metapb.Column {
	var pkCols []*metapb.Column
	for _, c := range t.Columns {
		if c.PrimaryKey > 0 {
			pkCols = append(pkCols, c)
		}
	}
	return pkCols
}

func isRangeInvalid(r *metapb.Range, start, limit []byte) bool {
	return bytes.Compare(r.StartKey, start) < 0 || bytes.Compare(r.StartKey, r.EndKey) >= 0 ||
		bytes.Compare(r.EndKey, limit) > 0 || len(r.Peers) == 0
}

func checkInvalidRanges(t *metapb.Table, ranges []*metapb.Range) (invalids []*metapb.Range) {
	sort.Slice(ranges, func(i, j int) bool {
		c := bytes.Compare(ranges[i].StartKey, ranges[j].StartKey)
		if c != 0 {
			return c < 0
		}
		return bytes.Compare(ranges[i].EndKey, ranges[j].EndKey) < 0
	})

	tstart, tlimit := tableKeyScope(t)
	for _, r := range ranges {
		if isRangeInvalid(r, tstart, tlimit) {
			invalids = append(invalids, r)
		}
	}
	return
}

func repairRanges(t *metapb.Table, ranges []*metapb.Range, idGener func() uint64, nodeSelector func() uint64) (correctRanges []*metapb.Range) {
	tstart, tlimit := tableKeyScope(t)
	if len(ranges) == 0 {
		return []*metapb.Range{
			&metapb.Range{
				Id:       idGener(),
				StartKey: tstart,
				EndKey:   tlimit,
				TableId:  t.GetId(),
				RangeEpoch: &metapb.RangeEpoch{
					ConfVer: 1,
					Version: 1,
				},
				PrimaryKeys: tablePKs(t),
				Peers: []*metapb.Peer{
					&metapb.Peer{
						Id:     idGener(),
						NodeId: nodeSelector(),
					},
				},
			},
		}
	}

	// sort by startKey, then by EndKey. if both keys is the same, take the larger rangeId first
	sort.Slice(ranges, func(i, j int) bool {
		c := bytes.Compare(ranges[i].StartKey, ranges[j].StartKey)
		if c != 0 {
			return c < 0
		}
		c = bytes.Compare(ranges[i].EndKey, ranges[j].EndKey)
		if c != 0 {
			return c < 0
		}
		return ranges[i].Id > ranges[j].Id
	})

	prevEnd := tstart
	for _, r := range ranges {
		if isRangeInvalid(r, tstart, tlimit) {
			fmt.Printf("remove invalid ranges %d: %v-%v.\n", r.Id, r.StartKey, r.EndKey)
			continue
		}

		if bytes.Compare(r.StartKey, prevEnd) >= 0 {
			newr := *r
			newr.StartKey = append([]byte(nil), prevEnd...)
			correctRanges = append(correctRanges, &newr)
			prevEnd = append([]byte(nil), newr.EndKey...)
		}
	}
	correctRanges[len(correctRanges)-1].EndKey = append([]byte(nil), tlimit...)

	if !bytes.Equal(correctRanges[0].StartKey, tstart) || !bytes.Equal(correctRanges[len(correctRanges)-1].EndKey, tlimit) {
		panic("wrong new ranges start key or end key")
	}
	for i := 0; i < len(correctRanges)-1; i++ {
		cr := correctRanges[i]
		nr := correctRanges[i+1]
		if bytes.Compare(cr.EndKey, cr.StartKey) <= 0 || bytes.Compare(nr.EndKey, nr.StartKey) <= 0 || !bytes.Equal(nr.StartKey, cr.EndKey) {
			panic("wrong new range keys order")
		}
	}
	return
}

func createNewDBFile(path string, dbs []*metapb.DataBase, tables []*metapb.Table, nodes []*metapb.Node, ranges []*metapb.Range) {
	newdb, err := bolt.Open(path, 0664, nil)
	if err != nil {
		panic(err)
	}
	newdb.NoSync = true
	err = newdb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dbBucket)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		// write dbs
		for _, d := range dbs {
			key := []byte(fmt.Sprintf("%s%d", dbPrefix, d.Id))
			value, err := d.Marshal()
			if err != nil {
				return err
			}
			err = b.Put(key, value)
			if err != nil {
				return err
			}
		}

		// write tables
		for _, t := range tables {
			key := []byte(fmt.Sprintf("%s%d", tablePrefix, t.Id))
			value, err := t.Marshal()
			if err != nil {
				return err
			}
			err = b.Put(key, value)
			if err != nil {
				return err
			}
		}

		// write nodes
		for _, n := range nodes {
			key := []byte(fmt.Sprintf("%s%d", nodePrefix, n.Id))
			value, err := n.Marshal()
			if err != nil {
				return err
			}
			err = b.Put(key, value)
			if err != nil {
				return err
			}
		}

		// write rangs
		for _, r := range ranges {
			key := []byte(fmt.Sprintf("%s%d", rangePrefix, r.Id))
			value, err := r.Marshal()
			if err != nil {
				return err
			}
			err = b.Put(key, value)
			if err != nil {
				return err
			}

		}

		// write max autoincr id
		idBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(idBuf, autoIncrID+1)
		return b.Put(incrIDKey, idBuf)
	})
	if err != nil {
		panic(err)
	}

	err = newdb.Sync()
	if err != nil {
		panic(err)
	}
}

func writeDSRangeMetaFile(path string, ranges []*metapb.Range) {
	fmt.Printf("total %d ranges...\n", len(ranges))
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(ranges)))
	_, err = f.Write(sizeBuf)
	if err != nil {
		panic(err)
	}

	for _, r := range ranges {
		data, err := r.Marshal()
		if err != nil {
			panic(err)
		}

		sizeBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(sizeBuf, uint64(len(data)))
		_, err = f.Write(sizeBuf)
		if err != nil {
			panic(err)
		}

		_, err = f.Write(data)
		if err != nil {
			panic(err)
		}
	}

	err = f.Sync()
	if err != nil {
		panic(err)
	}
}

func main() {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}

	nodeSelector := getNodeSelector(db)
	idGenerator := getIDGenerator(db)

	dbs := getDBs(db)
	_ = dbs

	_ = nodeSelector
	_ = idGenerator

	var allCorrectRanges []*metapb.Range
	tables := getTables(db)
	for _, t := range tables {
		ranges := getTableRanges(db, t.GetId())
		fmt.Printf("\n\ntable %s.%s has %d ranges.\n", t.GetDbName(), t.GetName(), len(ranges))
		correctRanges := repairRanges(t, ranges, idGenerator, nodeSelector)
		fmt.Printf("corrected ranges: %d\n\n", len(correctRanges))
		allCorrectRanges = append(allCorrectRanges, correctRanges...)
	}

	createNewDBFile("./repaired.db", getDBs(db), tables, getNodes(db), allCorrectRanges)
	writeDSRangeMetaFile("./range.meta", allCorrectRanges)
}
