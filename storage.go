package main

import (
	"fmt"
	"github.com/gocql/gocql"
)

type Storage struct {
	Session *gocql.Session
	File    string
}

func (s *Storage) initStorage() error {
	if err := s.Session.Query("CREATE KEYSPACE IF NOT EXISTS dconf WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };").Exec(); err != nil {
		return err
	}
	if err := s.Session.Query(`CREATE TABLE IF NOT EXISTS dconf.files ( 
          entryname text,
		  block     int,
		  data      blob,
		  hash      text,
          PRIMARY KEY(entryname, block));`).Exec(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) getHashes() ([]string, error) {
	iter := s.Session.Query("SELECT block, hash FROM dconf.files WHERE entryname=?;", s.File).Iter()
	res := make([]string, 0)
	var block int
	var hash string
	for iter.Scan(&block, &hash) {
		res = set(res, block, hash)
	}
	return res, nil
}

func (s *Storage) setHashes(hashes []string) error {
	if err := s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", s.File).Exec(); err != nil {
		return err
	}

	for i, h := range hashes {
		if err := s.Session.Query("INSERT INTO dconf.files(entryname, block, data, hash) VALUES (?,?,?,?);", s.File, i, make([]byte, 0), h).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) readChunk(h string) ([]byte, error) {
	log.Debug("Storage:readChunk(%s)", h)
	iter := s.Session.Query("SELECT data FROM dconf.files WHERE entryname=? AND block=0;", s.File+":"+h).Iter()
	var data []byte
	for iter.Scan(&data) {
		return data, nil
	}
	return nil, fmt.Errorf("File %s: Chunk %s not found", s.File, h)
}

func (s *Storage) writeChunk(h string, data []byte) error {
	log.Debug("Storage:writeChunk(%s,%d)", h, len(data))
	if err := s.Session.Query("INSERT INTO dconf.files(entryname, block, data, hash) VALUES (?,?,?,?);", s.File+":"+h, 0, data, "").Exec(); err != nil {
		return err
	}
	return nil
}
