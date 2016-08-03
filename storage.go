package main

import (
	"fmt"
  "strconv"
  "time"
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
  var hash string
  res := make([]string, 0)
	var block int

  // v2: files[entryname=?,block=-1].hash points to a reference to a hash list
  iter_v2 := s.Session.Query("SELECT hash FROM dconf.files WHERE entryname=? AND block=?;", s.File, -1).Iter()
  for iter_v2.Scan(&hash) {
    if err := iter_v2.Close(); err != nil {
      return nil, err
    }

    iter_v2_1 := s.Session.Query("SELECT block, hash FROM dconf.files WHERE entryname=?;", hash).PageSize(256).Iter()
    for iter_v2_1.Scan(&block, &hash) {
  		res = set(res, block, hash)
  	}
    if err := iter_v2_1.Close(); err != nil {
      return nil, err
    }

  	return res, nil
  }

  // v1: don't use indirect addressing of hash lists
	iter := s.Session.Query("SELECT block, hash FROM dconf.files WHERE entryname=?;", s.File).PageSize(256).Iter()

	for iter.Scan(&block, &hash) {
		res = set(res, block, hash)
	}
  if err := iter.Close(); err != nil {
    return nil, err
  }
	return res, nil
}

func (s *Storage) setHashes(oldHashes, hashes []string) error {
  now := time.Now()
  new_ref := s.File+":*"+strconv.FormatInt(now.Unix(), 10)

  var old_ref string
  var old_version bool = true

  iter_v2 := s.Session.Query("SELECT hash FROM dconf.files WHERE entryname=? AND block=?;", s.File, -1).Iter()
  for iter_v2.Scan(&old_ref) {
    old_version = false
  }
  if err := iter_v2.Close(); err != nil {
    return err
  }

	newHashes := make(map[string]bool)

	for i := 0; i < len(hashes); i++ {
		if err := s.Session.Query("INSERT INTO dconf.files(entryname, block, data, hash) VALUES (?,?,?,?);",
      new_ref, i, make([]byte, 0), hashes[i]).Exec(); err != nil {
      orig_err := err
      s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", new_ref).Exec()
      return orig_err
    }
		newHashes[hashes[i]] = true
	}

  if err := s.Session.Query("INSERT INTO dconf.files(entryname, block, data, hash) VALUES (?,?,?,?);",
     s.File, -1, make([]byte, 0), new_ref).Exec(); err != nil {
    orig_err := err
    s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", new_ref).Exec()
    return orig_err
  }

  if old_version {
	  for i := len(hashes); i < len(oldHashes); i++ {
		  s.Session.Query("DELETE FROM dconf.files WHERE entryname=? AND block=?;", s.File, i).Exec()
	  }
  } else {
    s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", old_ref).Exec()
  }

	for _, h := range oldHashes {
		if _, ok := newHashes[h]; !ok {
			s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", s.File+":"+h).Exec()
		}
	}

	//if err := s.Session.Query("DELETE FROM dconf.files WHERE entryname=?;", s.File).Exec(); err != nil {
	//	return err
	//}

	//for i, h := range hashes {
	//	if err := s.Session.Query("INSERT INTO dconf.files(entryname, block, data, hash) VALUES (?,?,?,?);", s.File, i, make([]byte, 0), h).Exec(); err != nil {
	//		return err
	//	}
	//}

	return nil
}

func (s *Storage) readChunk(h string) ([]byte, error) {
	log.Debug("Storage:readChunk(%s)", h)
	iter := s.Session.Query("SELECT data FROM dconf.files WHERE entryname=? AND block=0;", s.File+":"+h).Iter()
  defer iter.Close()
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
