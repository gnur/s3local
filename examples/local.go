package main

import (
	"fmt"

	"github.com/gnur/s3local"
)

func main() {
	store, err := s3local.New(s3local.Config{
		Type: "local",
		Settings: map[string]string{
			"path": "/Users/erwin/tmp",
		},
	})
	if err != nil {
		return
	}
	fmt.Println(store.List("test"))
}
