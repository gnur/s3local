package s3

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go"
	log "github.com/sirupsen/logrus"
)

// Store stores the config needed for a local store
type Store struct {
	Bucket string
	Client *minio.Client
}

// Type returns the Store Type [s3, local]
func (s Store) Type() string {
	return "s3"
}

// Read returns the contents of an object at key
func (s Store) Read(key string) ([]byte, error) {
	object, err := s.Client.GetObject(s.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return []byte{}, err
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object)
	if err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil

}

// List all objects with the prefix given
func (s Store) List(prefix, suffix string) ([]string, error) {
	var files []string
	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	isRecursive := true
	objectCh := s.Client.ListObjectsV2(s.Bucket, prefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
		}
		if strings.HasSuffix(object.Key, suffix) {
			files = append(files, object.Key)
		}
	}
	return files, nil
}

// Write writes the contents of content to key
func (s Store) Write(key string, content io.Reader) error {
	_, err := s.Client.PutObject(s.Bucket, key, content, -1, minio.PutObjectOptions{})
	return err
}

// New returns a new local store implementing the s3local interface
func New(settings map[string]string) (Store, error) {
	var host string
	var bucket string
	var accessKeyID string
	var secretAccessKey string

	if _, ok := settings["host"]; ok {
		host = settings["host"]
	} else {
		host = "s3.amazonaws.com"
	}
	log.WithField("host", host).Debug("using host")

	if _, ok := settings["secretaccesskey"]; ok {
		secretAccessKey = settings["secretaccesskey"]
	} else {
		return Store{}, errors.New("secretaccesskey not provided")
	}

	if _, ok := settings["bucket"]; ok {
		bucket = settings["bucket"]
	} else {
		return Store{}, errors.New("bucket not provided")
	}

	if _, ok := settings["accesskeyid"]; ok {
		accessKeyID = settings["accesskeyid"]
	} else {
		return Store{}, errors.New("accesskeyid not provided")
	}

	minioClient, err := minio.New(host, accessKeyID, secretAccessKey, true)
	if err != nil {
		log.WithField("err", err).Debug("Could not create client")
		return Store{}, err
	}
	log.WithField("mc", minioClient).Debug("created client")

	return Store{
		Bucket: bucket,
		Client: minioClient,
	}, nil
}
