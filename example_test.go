package wal_test

import (
	"fmt"
	"log"
	"os"

	"wal"
)

func Example() {
	dir, err := os.MkdirTemp("", "wal")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	l, err := wal.Open(dir, wal.Options{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := l.Append([]byte("set a"), []byte("set b"), []byte("del a")); err != nil {
		log.Fatal(err)
	}

	// A consumer resumes at the checkpoint and commits past what it handled.
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

	fmt.Println("resume at", l.Committed())

	if err := l.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// 1 set a
	// 2 set b
	// resume at 3
}
