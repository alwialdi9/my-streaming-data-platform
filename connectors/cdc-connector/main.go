package main

import (
	"context"
	"log"
	"time"

	// "github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	slotName     = "app_slot"
	publication  = "app_pub"
	outputPlugin = "pgoutput"
)

// pg_recvlogical -d postgresql://macbookpro:macbookpro@localhost:5432/macbookpro -S app_slot -P pgoutput --start -f - -o proto_version=2 -o publication_names=app_pub
func main() {
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx,
		"postgresql://macbookpro:macbookpro@localhost:5432/macbookpro?replication=database",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		slotName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + publication + "'",
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Logical replication started on slot", slotName)

	log.Println("CDC replication started")

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	// relations := map[uint32]*pglogrepl.RelationMessage{}
	// relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	// typeMap := pgtype.NewMap()
	// clientXLogPos := sysident.XLogPos

	decoder := NewDecoder()

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}

			events := decoder.Decode(xld.WALData, xld.WALStart)
			for _, evt := range events {
				log.Printf("CDC EVENT: %+v\n", evt)
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}
