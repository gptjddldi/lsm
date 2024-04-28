package main

import (
	"fmt"
	"log"
	"lsm/db"
)

func main() {
	const dataFolder = "demo-data"
	d, err := db.Open(dataFolder)
	if err != nil {
		log.Fatal(err)
	}
	//for i := 0; i < 100000; i++ {
	//	d.Insert([]byte("key"+string(i)), []byte("value"+string(i)))
	//}
	val, err := d.Get([]byte("key" + string(1991)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(string(val))
}
