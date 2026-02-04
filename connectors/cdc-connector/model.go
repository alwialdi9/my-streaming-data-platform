package main

import "github.com/jackc/pglogrepl"

type ChangeEvent struct {
	Op     string                 `json:"op"` // c,u,d,r
	Schema string                 `json:"schema"`
	Table  string                 `json:"table"`
	Before map[string]interface{} `json:"before,omitempty"`
	After  map[string]interface{} `json:"after,omitempty"`
	LSN    string                 `json:"lsn"`
}

func tupleToMap(
	meta *TableMeta,
	tuple *pglogrepl.TupleData,
) map[string]interface{} {
	if tuple == nil {
		return nil
	}

	result := make(map[string]interface{})
	for i, col := range tuple.Columns {
		colName := meta.Columns[i].Name

		switch col.DataType {
		case 'n': // NULL
			result[colName] = nil

		case 't': // text format
			result[colName] = decodeText(meta.Columns[i].OID, col.Data)

		case 'u': // unchanged TOAST
			result[colName] = "__unchanged__"
		}
	}
	return result
}
