package main

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

type Cache struct {
	CacheDir  string
	ChunkSize int64
}

func (c *Cache) getCachedHashes() ([]string, error) {
	files, err := ioutil.ReadDir(c.CacheDir)
	if err != nil {
		return nil, err
	}
	var res = make([]string, 0)
	for _, fi := range files {
		if !fi.IsDir() {
			res = append(res, fi.Name())
		}
	}
	return res, nil
}

func (c *Cache) writeChunk(h string, data []byte) error {
	return ioutil.WriteFile(path.Join(c.CacheDir, h), data, 0655)
}

func (c *Cache) removeChunk(h string) error {
	return os.Remove(path.Join(c.CacheDir, h))
}

func (c *Cache) openChunk(h string) (io.ReadCloser, error) {
	return os.Open(path.Join(c.CacheDir, h))
}

func (c *Cache) initCache() error {
	return os.MkdirAll(c.CacheDir, 0755)
}
