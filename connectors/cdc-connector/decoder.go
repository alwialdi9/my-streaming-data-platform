package main

import (
	"github.com/jackc/pglogrepl"
)

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
}

func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

func (d *Decoder) Decode(data []byte, lsn pglogrepl.LSN) []ChangeEvent {
	var events []ChangeEvent

	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return events
	}

	switch m := msg.(type) {

	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m

	case *pglogrepl.InsertMessage:
		rel := d.relations[m.RelationID]
		events = append(events, ChangeEvent{
			Op:    "c",
			Table: rel.RelationName,
			After: tupleToMap(rel, m.Tuple),
			LSN:   lsn.String(),
		})

	case *pglogrepl.UpdateMessage:
		rel := d.relations[m.RelationID]
		events = append(events, ChangeEvent{
			Op:     "u",
			Table:  rel.RelationName,
			Before: tupleToMap(rel, m.OldTuple),
			After:  tupleToMap(rel, m.NewTuple),
			LSN:    lsn.String(),
		})

	case *pglogrepl.DeleteMessage:
		rel := d.relations[m.RelationID]
		events = append(events, ChangeEvent{
			Op:     "d",
			Table:  rel.RelationName,
			Before: tupleToMap(rel, m.OldTuple),
			LSN:    lsn.String(),
		})
	}

	return events
}
