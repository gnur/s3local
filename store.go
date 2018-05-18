package s3local

import (
	"errors"
	"fmt"

	"github.com/gnur/s3local/local"
)

var (
	// ErrObjectNotFound Error of not finding a file(object)
	ErrObjectNotFound = fmt.Errorf("object not found")
	// ErrObjectExists error trying to create an already existing file.
	ErrObjectExists = fmt.Errorf("object already exists in backing store (use store.Get)")
	// ErrNotImplemented this feature is not implemented for this store
	ErrNotImplemented = fmt.Errorf("Not implemented")
)

type (
	// S3Local interface to define the Storage abstracting
	S3Local interface {
		// Type returns the Store Type [s3, local]
		Type() string

		// Read returns the contents of an object at key
		Read(key string) ([]byte, error)

		// List all objects with the prefix given
		List(prefix string) ([]string, error)

		// Write writes the contents of content to key
		Write(key string, content []byte) error
	}

	// Config holds the configuration needed for a s3local storage
	Config struct {
		Type string

		Settings map[string]string
	}
)

// New returns a new store
func New(conf Config) (S3Local, error) {
	if conf.Type == "s3" {
		fmt.Println("s3")
		return nil, errors.New("not yet implemented")
	} else if conf.Type == "local" {
		fmt.Println("local")
		s, _ := local.New(conf.Settings)
		return s, nil
	} else {
		return nil, errors.New("invalid type")
	}
}
