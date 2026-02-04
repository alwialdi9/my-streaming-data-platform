package main

import (
	"context"
	"log"
	"strconv"

	"github.com/jackc/pglogrepl"
)

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	cache     *MetadataCache
}

func NewDecoder() *Decoder {
	return &Decoder{relations: map[uint32]*pglogrepl.RelationMessage{}, cache: &MetadataCache{tables: make(map[uint32]*TableMeta)}}
}

func (d *Decoder) Decode(wal []byte, lsn pglogrepl.LSN, producer *KafkaProducer, ctx context.Context) []ChangeEvent {
	var out []ChangeEvent

	msg, err := pglogrepl.Parse(wal)
	if err != nil {
		return out
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m
		meta := &TableMeta{
			Schema: m.Namespace,
			Table:  m.RelationName,
		}

		for _, col := range m.Columns {
			meta.Columns = append(meta.Columns, ColumnMeta{
				Name: col.Name,
				OID:  col.DataType,
			})
		}
		// fetch PK dynamically
		meta.PK = fetchPrimaryKey(meta.Schema, meta.Table)
		d.cache.tables[m.RelationID] = meta

		// ensureKafkaTopic(meta)

	case *pglogrepl.InsertMessage:
		// rel := d.relations[m.RelationID]
		out = append(out, buildInsertEvent(d.cache.tables[m.RelationID], m, lsn))
		err := producer.Produce(buildInsertEvent(d.cache.tables[m.RelationID], m, lsn), d.cache.tables[m.RelationID])
		if err != nil {
			log.Fatal(err)
		}

	case *pglogrepl.UpdateMessage:
		// rel := d.relations[m.RelationID]
		meta := d.cache.tables[m.RelationID]
		out = append(out, buildUpdateEvent(meta, m, lsn))
		err := producer.Produce(buildUpdateEvent(meta, m, lsn), meta)
		if err != nil {
			log.Fatal(err)
		}

	case *pglogrepl.DeleteMessage:
		// rel := d.relations[m.RelationID]
		meta := d.cache.tables[m.RelationID]
		out = append(out, buildDeleteEvent(meta, m, lsn))
		err := producer.Produce(buildDeleteEvent(meta, m, lsn), meta)
		if err != nil {
			log.Fatal(err)
		}
	}
	return out
}

func buildInsertEvent(
	meta *TableMeta,
	msg *pglogrepl.InsertMessage,
	lsn pglogrepl.LSN,
) ChangeEvent {
	after := tupleToMap(meta, msg.Tuple)
	return ChangeEvent{
		Op:     "c",
		Schema: meta.Schema,
		Table:  meta.Table,
		After:  after,
		LSN:    lsn.String(),
	}
}

func buildUpdateEvent(
	meta *TableMeta,
	msg *pglogrepl.UpdateMessage,
	lsn pglogrepl.LSN,
) ChangeEvent {
	return ChangeEvent{
		Op:     "u",
		Schema: meta.Schema,
		Table:  meta.Table,
		Before: tupleToMap(meta, msg.OldTuple),
		After:  tupleToMap(meta, msg.NewTuple),
		LSN:    lsn.String(),
	}
}

func buildDeleteEvent(
	meta *TableMeta,
	msg *pglogrepl.DeleteMessage,
	lsn pglogrepl.LSN,
) ChangeEvent {

	return ChangeEvent{
		Op:     "d",
		Schema: meta.Schema,
		Table:  meta.Table,
		Before: tupleToMap(meta, msg.OldTuple),
		LSN:    lsn.String(),
	}
}

func decodeText(oid uint32, data []byte) interface{} {
	s := string(data)

	switch oid {
	case 16: // bool
		return s == "t"

	case 20, 21, 23: // int8, int2, int4
		v, _ := strconv.ParseInt(s, 10, 64)
		return v

	case 700, 701: // float4, float8
		v, _ := strconv.ParseFloat(s, 64)
		return v

	case 1700: // numeric
		return s // biarkan string (presisi)

	default:
		return s
	}
}

func fetchPrimaryKey(schema, table string) []string {
	type PKRow struct {
		ColumnName string `gorm:"column:attname"`
	}
	rows := []PKRow{}
	var pk []string

	err := DB.Raw(`
      SELECT a.attname
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid
      WHERE i.indisprimary
        AND n.nspname = $1
        AND c.relname = $2
        AND a.attnum = ANY(i.indkey)
      ORDER BY a.attnum
    `, schema, table).Scan(&rows).Error

	if err != nil {
		log.Printf("error when get pg_index: %s", err.Error())
	}

	for _, r := range rows {
		pk = append(pk, r.ColumnName)
	}
	return pk
}
