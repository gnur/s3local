package local

import (
	"os"
	"path"
	"path/filepath"
	"strings"
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
	return []byte{}, nil
}

// List all objects with the prefix given
func (s Store) List(prefix string) ([]string, error) {
	var files []string
	walkPath := s.Path
	testprefix := path.Join(s.Path, prefix)
	info, err := os.Stat(testprefix)
	if err == nil && info.IsDir() {
		walkPath = testprefix
	}

	err = filepath.Walk(walkPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasPrefix(path, testprefix) {
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
func (s Store) Write(key string, content []byte) error {
	return nil
}

func New(settings map[string]string) (Store, error) {
	return Store{
		Path: settings["path"],
	}, nil
}
