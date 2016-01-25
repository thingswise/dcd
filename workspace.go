package main

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"
)

type Workspace struct {
	Root string
}

func (w *Workspace) getEntry(name string) string {
	return path.Join(w.Root, name)
}

func (w *Workspace) checkoutMarker() string {
	return path.Join(w.Root, ".dcd")
}

func (w *Workspace) writeRegFile(filePath string, mode os.FileMode, modTime time.Time, r io.Reader) error {
	d, err := os.Create(filePath)
	if err != nil {
		return err
	}
	n, err := io.Copy(d, r)
	if err != nil {
		return err
	}

	log.Debug("Adding %s (%d bytes)", filePath, n)

	os.Chtimes(filePath, time.Now(), modTime)

	if err := os.Chmod(filePath, mode&0777555); err != nil {
		return err
	}

	return nil
}

func (w *Workspace) WriteEntry(name string, mode os.FileMode, modTime time.Time, r io.Reader, replace bool) error {
	os.MkdirAll(w.Root, 0755)
	filePath := w.getEntry(name)
	info, err := os.Stat(filePath)
	if err != nil {
		if mode.IsDir() {
			if err := os.MkdirAll(filePath, mode); err != nil {
				return err
			}
		} else {
			if err := w.writeRegFile(filePath, mode, modTime, r); err != nil {
				return err
			}
		}
	} else {
		if info.IsDir() {
			if mode.IsDir() {
				if err := os.Chmod(filePath, mode&0777555); err != nil {
					return err
				}
			} else {
				if info.ModTime().Before(modTime) || replace {
					os.RemoveAll(filePath)
					if err := w.writeRegFile(filePath, mode, modTime, r); err != nil {
						return err
					}
				} else {
					// pass
				}
			}
		} else {
			if mode.IsDir() {
				if info.ModTime().Before(modTime) || replace {
					os.RemoveAll(filePath)
					if err := os.MkdirAll(filePath, mode); err != nil {
						return err
					}
				} else {
					// pass
				}
			} else {
				if info.ModTime().Before(modTime) || replace {
					if err := w.writeRegFile(filePath, mode, modTime, r); err != nil {
						return err
					}
				} else {
					// pass
				}
			}
		}
	}

	return nil
}

type RemoveFilterFunc func(string) bool

func (w *Workspace) Remove(name string) {
	os.Remove(w.getEntry(name))
}

func (w *Workspace) RemoveAll(f RemoveFilterFunc) {
	filepath.Walk(w.Root, func(name string, info os.FileInfo, err error) error {
		filePath, err := filepath.Rel(w.Root, name)
		if err != nil {
			panic(err.Error())
		}

		if err != nil {
			return err
		}

		if filePath == "." || filePath == ".dcd" {
			// skip self
			return nil
		}

		if f(filePath) {
			log.Debug("Removing %s", filePath)
			os.RemoveAll(name)
		}

		return nil
	})
}

func (w *Workspace) GetCheckout() (string, error) {
	if _, err := os.Stat(w.checkoutMarker()); err == nil {
		b, err := ioutil.ReadFile(w.checkoutMarker())
		if err != nil {
			return "", err
		}

		return string(b), nil
	} else {
		return "", nil
	}
}

func (w *Workspace) SetCheckout(chk string) error {
	os.MkdirAll(w.Root, 0755)
	return ioutil.WriteFile(w.checkoutMarker(), []byte(chk), 0644)
}

func (w *Workspace) RemoveCheckout() error {
	return os.Remove(w.checkoutMarker())
}

type WalkFunc func(path string, info os.FileInfo, r io.Reader, err error) error

func (w *Workspace) Walk(f WalkFunc) error {
	return filepath.Walk(w.Root, func(path string, info os.FileInfo, err error) error {
		filePath, err := filepath.Rel(w.Root, path)
		if err != nil {
			panic(err.Error())
		}

		log.Debug("Walk: walking file %s", filePath)

		if err != nil {
			return f(filePath, info, nil, err)
		}

		if info.IsDir() {
			return f(filePath, info, nil, nil)
		} else {
			r, err2 := os.Open(path)
			if err2 != nil {
				return f(filePath, info, nil, err2)
			}

			defer r.Close()
			return f(filePath, info, r, nil)
		}

	})
}

func (w *Workspace) MakeWritable() error {
	return filepath.Walk(w.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		return os.Chmod(path, info.Mode()|0222)
	})
}

func (w *Workspace) MakeReadonly() error {
	return filepath.Walk(w.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		return os.Chmod(path, info.Mode()&0777555)
	})
}
