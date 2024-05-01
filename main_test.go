// aesse: best for sequential search
// magnamet : best for binary search
// voluptatemqui : worst

package main_test

import (
	"fmt"
	"io"
	"log"
	"lsm/db"
	"testing"
)

var keys = []string{
	"aesse",         // best for sequential search
	"magnamet",      // best for binary search
	"voluptatemqui", // worth for both search
}

func init() {
	log.SetOutput(io.Discard)
}

func BenchmarkSSTSearch(b *testing.B) {
	d, err := db.Open("demo-data")
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range keys {
		b.Run(fmt.Sprintln(k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err = d.Get([]byte(k))
				if err != nil {
					b.Fatal(err.Error())
				}
			}
		})
	}
}
