package main

import (
	"bytes"
	"os"

	"github.com/jackc/pglogrepl"
)

type OffsetStore interface {
	Load() (pglogrepl.LSN, error)
	Save(pglogrepl.LSN) error
}

type FileOffsetStore struct{ path string }

func (f *FileOffsetStore) Load() (pglogrepl.LSN, error) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(string(bytes.TrimSpace(b)))
}

func (f *FileOffsetStore) Save(lsn pglogrepl.LSN) error {
	return os.WriteFile(f.path, []byte(lsn.String()), 0644)
}
