package main

type TableMeta struct {
	Schema  string
	Table   string
	Columns []ColumnMeta
	PK      []string
}

type ColumnMeta struct {
	Name string
	OID  uint32
}

type MetadataCache struct {
	tables map[uint32]*TableMeta // key = relationID
}
