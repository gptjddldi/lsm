package main

import (
	"bufio"
	"lsm/cli"
	"lsm/skiplist"
	"os"
)

func main() {
	sl := skiplist.NewSkipList()
	scanner := bufio.NewScanner(os.Stdin)
	demo := cli.NewCLI(scanner, sl)
	demo.Start()
}
