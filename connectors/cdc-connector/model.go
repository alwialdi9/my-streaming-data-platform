package main

import "github.com/jackc/pglogrepl"

type ChangeEvent struct {
	Op     string                 `json:"op"` // c,u,d,r
	Table  string                 `json:"table"`
	Before map[string]interface{} `json:"before,omitempty"`
	After  map[string]interface{} `json:"after,omitempty"`
	LSN    string                 `json:"lsn"`
}

func tupleToMap(
	rel *pglogrepl.RelationMessage,
	tuple *pglogrepl.TupleData,
) map[string]interface{} {

	if tuple == nil {
		return nil
	}

	result := make(map[string]interface{})
	for i, col := range tuple.Columns {
		colName := rel.Columns[i].Name
		result[colName] = string(col.Data)
	}
	return result
}
