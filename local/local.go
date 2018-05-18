package local

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Store stores the config needed for a local store
type Store struct {
	Path string
}

// Type returns the Store Type [s3, local]
func (s Store) Type() string {
	return "local"
}

// Read returns the contents of an object at key
func (s Store) Read(key string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(s.Path, key))
}

// List all objects with the prefix given
func (s Store) List(prefix, suffix string) ([]string, error) {
	var files []string
	walkPath := s.Path
	testprefix := path.Join(s.Path, prefix)
	info, err := os.Stat(testprefix)
	if err == nil && info.IsDir() {
		walkPath = testprefix
	}

	err = filepath.Walk(walkPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasPrefix(path, testprefix) && strings.HasSuffix(path, suffix) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return files, err
	}

	return files, nil
}

// Write writes the contents of content to key
func (s Store) Write(key string, content io.Reader) error {
	writePath := path.Join(s.Path, key)
	parentDir := path.Dir(writePath)
	log.WithField("parentDir", parentDir).Debug("checking parent dir")
	_, err := os.Stat(parentDir)
	log.WithField("err", err).Debug("os.stat")
	if err != nil {
		err = os.MkdirAll(parentDir, 0755)
		log.WithField("err", err).Debug("mkdirall")
		if err != nil {
			return err
		}
	}
	log.Debug("Writing file")
	f, err := os.Create(writePath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(content)

	_, err = f.Write(buf.Bytes())
	return err
}

// New returns a new local store implementing the s3local interface
func New(settings map[string]string) (Store, error) {
	return Store{
		Path: settings["path"],
	}, nil
}
