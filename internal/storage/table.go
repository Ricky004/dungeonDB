package storage

import ()

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

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
