package main

import (
	"log"
	"lsm/db"
)

func main() {
	const dataFolder = "demo-data"
	d, err := db.Open(dataFolder)
	if err != nil {
		log.Fatal(err)
	}
	d.Insert([]byte("key1"), []byte("value1"))
}
