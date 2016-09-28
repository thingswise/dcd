package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type System struct {
	s    *Storage
	c    *Cache
	w    *Workspace
	lock *sync.Mutex
}

func NewSystem(s *Storage, c *Cache, w *Workspace) *System {
	return &System{
		s:    s,
		c:    c,
		w:    w,
		lock: &sync.Mutex{},
	}
}

func (sys *System) Edit(forceOverwrite bool, ph *ProgressHandler) error {
	sys.lock.Lock()
	defer sys.lock.Unlock()

	s := sys.s
	c := sys.c
	w := sys.w

	var hash string

	chk, err := w.GetCheckout()
	if err != nil {
		return NewOperationError(InternalError, err.Error())
	}

	if forceOverwrite {
		hash, err = updateWorkspace(s, c, w, true, true, ph)
		if err != nil {
			log.Errorf("Cannot update workspace: %s", err.Error())
			return NewOperationError(InternalError, err.Error())
		}
		if chk != "" {
			w.RemoveCheckout()
		}
	} else {
		if chk != "" {
			return NewOperationError(AlreadyCheckedOut, "The workspace has already been checked out")
		}
		hash, err = updateWorkspace(s, c, w, true, false, ph)
		if err != nil {
			log.Errorf("Cannot update workspace: %s", err.Error())
			return NewOperationError(InternalError, err.Error())
		}
	}

	if err := w.SetCheckout(hash); err != nil {
		log.Errorf("Cannot set checkout marker: %s", err.Error())
		return NewOperationError(InternalError, err.Error())
	}

	//if err := w.MakeWritable(); err != nil {
	//	log.Errorf("Cannot make writable: %s", err.Error())
	//	return NewOperationError(InternalError, err.Error())
	//}

	return nil
}

func (sys *System) Commit(forceOverwrite bool, ph *ProgressHandler) error {
	sys.lock.Lock()
	defer sys.lock.Unlock()

	if ph != nil {
		ph.SetTotal(-1)
	}

	s := sys.s
	c := sys.c
	w := sys.w

	hashes, err := s.getHashes()
	if err != nil {
		log.Errorf("Cannot get hash list from DB: %s", err.Error())
		return NewOperationError(InternalError, "Cannot get the hash list from DB")
	}

	if !forceOverwrite {
		checkout, err := w.GetCheckout()
		if err != nil {
			log.Errorf("Error getting checkout info: %s", err.Error())
			return NewOperationError(InternalError, err.Error())
		}

		if checkout == "" {
			return NewOperationError(NotCheckedOut, "The workspace has not been checked out")
		}

		hash := sha256.New()
		for _, h := range hashes {
			b, err := parseHashStr(h)
			if err != nil {
				log.Errorf("Cannot parse hash: %s", h)
				return NewOperationError(InternalError, "Cannot parse hash")
			}

			hash.Write(b)
		}

		currentCheckout := hashToStr(hash.Sum(make([]byte, 0)))
		if currentCheckout != checkout {
			return NewOperationError(CheckoutMismatch, "Workspace has been changed. Use -f to override")
		}
	}

	piper, pipew := io.Pipe()

	gzipStream := gzip.NewWriter(pipew)
	tarStream := tar.NewWriter(gzipStream)

	go func() {
		if err := w.Walk(func(path string, info os.FileInfo, r io.Reader, err error) error {
			if err != nil {
				return err
			}

			if path == "." || path == ".dcd" {
				return nil
			}

			header, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}

			header.Name = path

			if err := tarStream.WriteHeader(header); err != nil {
				return err
			}

			if !info.IsDir() {
				if _, err := io.Copy(tarStream, r); err != nil {
					pipew.CloseWithError(err)
					return err
				}
			}
			return nil
		}); err != nil {
			log.Errorf("Error building archive: %s", err.Error())
		}
		log.Debug("Tarring finished, closing")
		tarStream.Close()
		gzipStream.Close()
		pipew.Close()
	}()

	hash := sha256.New()

	var newHashes []string = make([]string, 0)

	var progress int64 = 0

	buf := make([]byte, c.ChunkSize)
	for {
		hash.Reset()
		n, err := io.ReadFull(piper, buf)
		if err == nil {
			log.Debug("Read full chunk (%d)", n)
			chunk := buf
			hash.Write(chunk)
			h := hashToStr(hash.Sum(nil))
			err2 := s.writeChunk(h, chunk)
			if err2 != nil {
				log.Errorf("Error writing chunk to DB: %s", err2.Error())
				piper.CloseWithError(err2)
				return NewOperationError(InternalError, err2.Error())
			}
			progress++
			if ph != nil {
				ph.SetProgress(progress)
			}
			newHashes = append(newHashes, h)
		} else if err == io.ErrUnexpectedEOF {
			// last chunk (incomplete)
			log.Debug("Read partial chunk (%d)", n)
			chunk := buf[0:n]
			hash.Write(chunk)
			log.Debug("Sum=%d", hash.Size())
			h := hashToStr(hash.Sum(nil))
			err2 := s.writeChunk(h, chunk)
			if err2 != nil {
				log.Errorf("Error writing chunk to DB: %s", err2.Error())
				piper.CloseWithError(err2)
				return NewOperationError(InternalError, err2.Error())
			}
			progress++
			if ph != nil {
				ph.SetProgress(progress)
			}
			newHashes = append(newHashes, h)
			break
		} else if err == io.EOF {
			log.Debug("No more chunks")
			// no more chunks
			break
		} else {
			log.Errorf("Error writing chunk: %s", err.Error())
			piper.CloseWithError(err)
			return NewOperationError(InternalError, err.Error())
		}
	}

	log.Debugf("Setting new hashes (%d)", len(newHashes))

	if err := s.setHashes(hashes, newHashes, func() {
		if ph != nil {
			progress++
			ph.SetProgress(progress)
		}
	}); err != nil {
		log.Errorf("Cannot update hash list: %s", err.Error())
		return NewOperationError(InternalError, err.Error())
	}

	w.RemoveCheckout()

	//if err := w.MakeReadonly(); err != nil {
	//	log.Errorf("Cannot make read-only: %s", err.Error())
	//	return NewOperationError(InternalError, err.Error())
	//}

	return nil
}

func (sys *System) Get(w io.Writer, ph *ProgressHandler) error {
	sys.lock.Lock()
	defer sys.lock.Unlock()

	s := sys.s

	hashes, err := s.getHashes()
	if err != nil {
		log.Errorf("Cannot get hash list from DB: %s", err.Error())
		return err
	}

	if ph != nil {
		ph.SetTotal(int64(len(hashes)))
	}

	var progress int64 = 0

	for _, h := range hashes {
		bytes, err := s.readChunk(h)
		if err != nil {
			return err
		}

		if _, err := w.Write(bytes); err != nil {
			return err
		}

		progress++
		if ph != nil {
			ph.SetProgress(progress)
		}
	}

	return nil
}

func (sys *System) Update(force bool, ph *ProgressHandler) error {
	sys.lock.Lock()
	defer sys.lock.Unlock()

	s := sys.s
	c := sys.c
	w := sys.w

	if _, err := updateWorkspace(s, c, w, true, force, ph); err != nil {
		log.Errorf("Cannot update workspace: %s", err.Error())
		return NewOperationError(InternalError, err.Error())
	}

	if force {
		w.RemoveCheckout()
	}

	return nil
}

func updateWorkspace(s *Storage, c *Cache, w *Workspace, forceUnpack bool, replace bool, ph *ProgressHandler) (string, error) {
	hashes, err := s.getHashes()
	if err != nil {
		log.Errorf("Cannot get hash list from DB: %s", err.Error())
		return "", err
	}
	hashSet := make(map[string]bool)
	for _, h := range hashes {
		hashSet[h] = true
	}
	//log.Debug("Have %d hashes in DB", len(hashes))

	cachedHashes, err := c.getCachedHashes()
	if err != nil {
		log.Errorf("Cannot get cached hash list: %s", err.Error())
		return "", err
	}
	cachedHashSet := make(map[string]bool)
	for _, h := range cachedHashes {
		cachedHashSet[h] = true
	}
	//log.Debug("Have %d hashes in workspace", len(hashes))

	var hashesToDownload int64 = 0

	for _, h := range hashes {
		if _, ok := cachedHashSet[h]; !ok {
			hashesToDownload++
		}
	}

	var hashesToUnpack int64 = 0

	if hashesToDownload > 0 || forceUnpack || len(hashes) != len(cachedHashes) {
		hashesToUnpack = int64(len(hashes))
	}

	var hashesToRemove int64 = 0

	for _, h := range cachedHashes {
		if _, ok := hashSet[h]; !ok {
			hashesToRemove++
		}
	}

	if ph != nil {
		ph.SetTotal(hashesToDownload + hashesToUnpack + hashesToRemove)
	}

	log.Debugf("hashesToDownload=%d, hashesToUnpack=%d, hashesToRemove=%d", hashesToDownload, hashesToUnpack, hashesToRemove)

	var progress int64 = 0

	var needUpdate = false
	for _, h := range hashes {
		if _, ok := cachedHashSet[h]; !ok {
			log.Debug("Need to download chunk %s", h)
			if err := downloadChunk(s, c, h); err != nil {
				log.Errorf("Cannot download chunk: %s", err.Error())
				return "", err
			}
			needUpdate = true
			progress++
			if ph != nil {
				ph.SetProgress(progress)
			}
		}
	}

	needUpdate = forceUnpack || needUpdate || len(hashes) != len(cachedHashes)
	if needUpdate {
		if err := unpack(hashes, c, w, replace); err != nil {
			log.Errorf("Cannot unpack: %s", err.Error())
			return "", err
		}
		progress += int64(len(hashes))
		if ph != nil {
			ph.SetProgress(progress)
		}
	}

	for _, h := range cachedHashes {
		if _, ok := hashSet[h]; !ok {
			if err := c.removeChunk(h); err != nil {
				log.Errorf("Cannot remove chunk: %s", err.Error())
			}
			progress++
			if ph != nil {
				ph.SetProgress(progress)
			}
		}
	}

	hash := sha256.New()
	for _, h := range hashes {
		b, err := parseHashStr(h)
		if err != nil {
			log.Errorf("Error parsing hash: %s", h)
			return "", err
		}
		hash.Write(b)
	}

	return hashToStr(hash.Sum(make([]byte, 0))), nil
}

func downloadChunk(s *Storage, c *Cache, h string) error {
	data, err := s.readChunk(h)
	if err != nil {
		return err
	}

	return c.writeChunk(h, data)
}

func unpack(hashes []string, c *Cache, w *Workspace, replace bool) error {
	chk, err := w.GetCheckout()
	if err != nil {
		return err
	}

	if !replace && chk != "" {
		log.Debug("Skipping unpack since the workspace has been checked out")
		return nil
	}

	existingEntries := make(map[string]bool)

	if len(hashes) > 0 {
		log.Debug("Unpacking %d chunks", len(hashes))
		var readers = make([]io.Reader, 0)
		for _, h := range hashes {
			f, err := c.openChunk(h)
			if err != nil {
				return err
			}

			defer f.Close()
			readers = append(readers, f)
		}

		joinedStreams := io.MultiReader(readers...)

		gzStream, err := gzip.NewReader(joinedStreams)
		if err != nil {
			return err
		}

		tarStream := tar.NewReader(gzStream)

		for {
			header, err := tarStream.Next()
			if err == io.EOF {
				break
			}

			if header == nil {
				return fmt.Errorf("Unexpected nil tar header")
			}

			var dir os.FileMode
			if header.Typeflag == '5' {
				dir = os.ModeDir
			} else if header.Typeflag == 0 || header.Typeflag == '0' {
				dir = 0
			} else {
				return fmt.Errorf("Unsupported header: %d", header.Typeflag)
			}

			existingEntries[header.Name] = true
			if err := w.WriteEntry(header.Name, dir|os.FileMode(header.Mode&0777755), header.ModTime, tarStream, replace); err != nil {
				return err
			}
		}
	}

	w.RemoveAll(func(path string) bool {
		_, ok := existingEntries[path]
		return !ok
	})

	return nil
}

func (sys *System) runUpdate() {
	sys.lock.Lock()
	defer sys.lock.Unlock()

	s := sys.s
	c := sys.c
	w := sys.w

	defer time.AfterFunc(5*time.Second, func() { sys.runUpdate() })

	updateWorkspace(s, c, w, false, false, nil)
}
