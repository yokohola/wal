package wal_test

import (
	"fmt"
	"log"
	"os"

	"gitlab.wildberries.ru/infrastructure/infrastructure-storage/userstorage/internal/service/replicator/wal"
)

func Example() {
	dir, err := os.MkdirTemp("", "wal")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	l, err := wal.Open(dir, wal.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	if _, err := l.Append([]byte("set a"), []byte("set b"), []byte("del a")); err != nil {
		log.Fatal(err)
	}

	// A consumer resumes at the checkpoint and commits what it has handled.
	recs, err := l.Read(l.Committed(), 2)
	if err != nil {
		log.Fatal(err)
	}

	for _, rec := range recs {
		fmt.Printf("%d %s\n", rec.Index, rec.Data)
	}

	if err := l.Commit(recs[len(recs)-1].Index + 1); err != nil {
		log.Fatal(err)
	}

	fmt.Println("committed:", l.Committed(), "last:", l.LastIndex())
	// Output:
	// 1 set a
	// 2 set b
	// committed: 3 last: 3
}
