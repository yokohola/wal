package wal_test

import (
	"context"
	"errors"
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

// dir and cfg configure the log the examples open.
var (
	dir = "/var/lib/myapp/wal"
	cfg = wal.Config{}
)

// Without SyncOnAppend a loop syncs periodically, and the log is reopened
// whenever it fails.
func Example_syncJob() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	l, err := wal.Open(dir, cfg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		err := syncLoop(ctx, l)
		if !errors.Is(err, wal.ErrPermanent) {
			break
		}

		// Stop the log's other users here, then switch them to the new log.
		if l, err = reopen(l); err != nil {
			log.Fatal(err)
		}
	}

	if err := l.Close(); err != nil {
		log.Fatal(err)
	}
}

// syncLoop makes appended records durable every 100ms until ctx ends or the log
// fails with ErrPermanent.
func syncLoop(ctx context.Context, l *wal.Log) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := l.Sync(); err != nil {
				return err
			}
		}
	}
}

// reopen replaces a log that failed with ErrPermanent. Stop every caller of the
// old log first and switch them to the new one.
func reopen(l *wal.Log) (*wal.Log, error) {
	log.Printf("wal failed, reopening: %v", l.Err())

	_ = l.Close() // returns the same error; the directory is released anyway

	return wal.Open(dir, cfg)
}
