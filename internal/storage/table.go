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
	Name  string   // table name
	Types []uint32 // column types
	Cols  []string // column names
	Pkeys int      // the first pkeys columns are primary keys
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
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
	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
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
	if tdef.Name == "" {
		return fmt.Errorf("table name is empty")
	}
	if len(tdef.Cols) == 0 {
		return fmt.Errorf("table columns are empty")
	}
	if len(tdef.Types) != len(tdef.Cols) {
		return fmt.Errorf("number of types does not match number of columns")
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
	return db.kv.UpdateW(key, val, mode)
}

// delete a record by its primary key
func DbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.Pkeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	return db.kv.DelW(key)
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

// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
func EscapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
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
