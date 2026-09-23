package wal_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/yokohola/wal"
)

// A producer appends and a consumer resumes from the checkpoint after every
// restart. Open creates the directory when it does not exist.
func Example() {
	l, err := wal.Open("/var/lib/myapp/wal", wal.Config{SyncOnAppend: true})
	if err != nil {
		log.Fatal(err)
	}

	for _, cmd := range []string{"set a", "set b"} {
		if _, err := l.Append([]byte(cmd)); err != nil {
			log.Fatal(err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := consume(ctx, l); err != nil {
		log.Print(err)
	}

	if err := l.Close(); err != nil {
		log.Fatal(err)
	}
}

func consume(ctx context.Context, l *wal.Log) error {
	for next := l.Committed() + 1; ctx.Err() == nil; {
		recs, err := l.Read(next, 1024)
		if err != nil {
			return err
		}

		if len(recs) == 0 {
			time.Sleep(10 * time.Millisecond)

			continue
		}

		for _, rec := range recs {
			log.Printf("apply %d: %s", rec.Index, rec.Data)
		}

		last := recs[len(recs)-1].Index
		next = last + 1

		if err := l.Commit(last); err != nil {
			return err
		}
	}

	return nil
}
