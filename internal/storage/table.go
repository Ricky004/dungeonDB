package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	u "github.com/Ricky004/dungeonDB/internal/utils"
)

// value types
const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// modes of the updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

// minimum value for table prefix
const TABLE_PREFIX_MIN = 3

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

type DB struct {
	Path string
	// internals
	kv     KV
	tables map[string]*TableDef // table name -> table definition
}

// table definition
type TableDef struct {
	// user defined
	Name    string     // table name
	Types   []uint32   // column types
	Cols    []string   // column names
	Pkeys   int        // the first pkeys columns are primary keys
	Indexes [][]string // secondary indexes
	// auto-assigned B-tree key prefixes for different tables
	Prefix        uint32
	IndexPrefixes []uint32
}

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	Pkeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	Pkeys:  1,
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	return nil
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	return nil
}

func (rec *Record) Get(key string) *Value {
	return nil
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return DbGet(db, tdef, rec)
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return DbUpdate(db, tdef, rec, mode)
}

// insert a record
func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

// update a record
func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

// upsert a record (insert or update)
func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

// delete a record
func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return DbDelete(db, tdef, rec)
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := DbGet(db, TDEF_TABLE, table)
	u.Assert(err == nil)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	u.Assert(tdef.Prefix == 0)
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = DbGet(db, TDEF_META, meta)
	u.Assert(err == nil)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		u.Assert(tdef.Prefix > TABLE_PREFIX_MIN)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	for i := range tdef.Indexes {
		prefix := tdef.Prefix + 1 + uint32(i)
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, prefix)
	}

	// update the next prefix
	ntree := 1 + uint32(len(tdef.Indexes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+ntree)
	_, err = DbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	u.Assert(err == nil)
	table.AddStr("def", val)
	_, err = DbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

// check the table definition
func tableDefCheck(tdef *TableDef) error {
	// verify the table definition
	if tdef.Name == "" {
		return fmt.Errorf("table name is empty")
	}
	if len(tdef.Cols) == 0 {
		return fmt.Errorf("table columns are empty")
	}
	if len(tdef.Types) != len(tdef.Cols) {
		return fmt.Errorf("number of types does not match number of columns")
	}
	// verify the indexes
	for i, index := range tdef.Indexes {
		index, err := CheckIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}

// get the table definition by name
func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := DbGet(db, TDEF_TABLE, rec)
	u.Assert(err == nil)
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	u.Assert(err == nil)
	return tdef
}

// get a single row by primary key
func DbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.Pkeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	val, ok := db.kv.GetW(key)
	if !ok {
		return false, nil
	}

	for i := tdef.Pkeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.Pkeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.Pkeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.Pkeys:]...)
	return true, nil
}

// add a row to the table
func DbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	val := EncodeValues(nil, values[tdef.Pkeys:])
	req := InsertReq{
		Key:  key,
		Val:  val,
		Mode: mode,
	}

	// Call the B-tree update function and check if the record was added
	added, err := db.kv.UpdateW(&req)
	if err != nil || !req.Updated || len(tdef.Indexes) == 0 {
		return added, err
	}

	// maintain the indexes
	if req.Updated && !req.Added {
		decodeValues(req.Old, values[tdef.Pkeys:]) // decode the old values
		indexOP(db, tdef, Record{tdef.Cols, values}, INDEX_DEL)
	}
	if req.Updated {
		indexOP(db, tdef, rec, INDEX_ADD)
	}
	return added, nil
}

// delete a record by its primary key
func DbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.Pkeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	req := DeleteReq{
		Key: key,
	}
	// Call the B-tree delete function
	deleted, err := db.kv.DelW(&req)
	if err != nil || !deleted || len(tdef.Indexes) == 0 {
		return deleted, err
	}

	// maintain the indexes
	if deleted {
		indexOP(db, tdef, rec, INDEX_DEL)
	}
	return true, nil
}

// indexOf returns the index of the first occurrence of str in slice, or -1 if not present.
func indexOf(slice []string, str string) int {
	for i, v := range slice {
		if v == str {
			return i
		}
	}
	return -1
}

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	if len(rec.Cols) != len(rec.Vals) {
		return nil,
			fmt.Errorf("record has %d columns but %d values", len(rec.Cols), len(rec.Vals))
	}

	if len(rec.Cols) < n {
		return nil,
			fmt.Errorf("record has %d columns but %d primary keys", len(rec.Cols), n)
	}

	values := make([]Value, len(tdef.Cols))
	for i, key := range rec.Cols {
		j := indexOf(tdef.Cols, key)
		if j < 0 {
			return nil,
				fmt.Errorf("column %s not found in table %s", key, tdef.Name)
		}
		values[j] = rec.Vals[i]
	}
	return values, nil
}

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = EncodeValues(out, vals)
	return out
}

// for encode values to bytes
// order-preserving encoding
func EncodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, EscapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("bad type")
		}
	}
	return out
}

// 1. strings are encoded as null-terminated strings,
// escape the null byte so that strings contain no null byte.
// 2. "\xff" represents the highest order in key comparisons,
// also escape the first byte if it's 0xff.
func EscapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}

	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	if len(in) > 0 && in[0] >= 0xfe {
       out[0] = 0xfe
	   out[1] = in[0]
	   pos += 2
	   in = in[1:]     
	}

	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

// for decode values from bytes
func decodeValues(in []byte, out []Value) {

}

func CheckIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}
	for _, c := range index {
		// check the index columns
		if _, ok := icols[c]; ok {
			return nil, fmt.Errorf("duplicate index column: %s", c)
		}
		icols[c] = true
	}
	// add the primary key to the index
	for _, c := range tdef.Cols[:tdef.Pkeys] {
		if !icols[c] {
			index = append(index, c)
		}
	}
	u.Assert(len(index) < len(tdef.Cols))
	return index, nil
}

func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.Pkeys]
	if isPrefix(pk, keys) {
		// use the primary key
		// also works for full table scans without a key.
		return -1, nil
	}

	// find a suitable index
	winner := -2
	for i, index := range tdef.Indexes {
		if !isPrefix(index, keys) {
			continue
		}
		if winner == -2 || len(index) < len(tdef.Indexes[winner]) {
			winner = i
		}
		if winner == -2 {
			return -2, fmt.Errorf("no index found")
		}
	}

	return winner, nil
}

func isPrefix(long, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i, c := range short {
		if long[i] != c {
			return false
		}
	}
	return true
}

// The range key can be a prefix of the index key,
// we may have to encode missing columns to make the comparison work.
func encodeKeyPartial(
	out []byte, prefix uint32, values []Value,
	tdef *TableDef, keys []string, cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	// Encode the missing columns as either minimum or maximum values,
	// depending on the comparison operator.
	// 1. The empty string is lower than all possible value encodings,
	// thus we don't need to add anything for CMP_LT and CMP_GE.
	// 2. The maximum encodings are all 0xff bytes.
	max := cmp == CMP_GT || cmp == CMP_LE
loop:
   for i := len(values); max && i < len(keys); i++ {
	switch tdef.Types[colIndex(tdef, keys[i])] {
	case TYPE_BYTES:
		out = append(out, 0xff)
		break loop // stops here since no string encoding starts with 0xff
	case TYPE_INT64:
		out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	default:
		fmt.Errorf("what?")
	}
   }
   return out
}


