package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Damage kinds applied to one record on disk.
const (
	badData       = "bad data"       // a flipped data byte
	badHeader     = "bad header"     // a flipped header byte
	zeroed        = "zeroed"         // the whole record reads as zeros
	partialHeader = "partial header" // a short write stopped inside the header
	partialData   = "partial data"   // a short write stopped inside the data
	stale         = "stale"          // a valid record of another index
	garbage       = "garbage"        // random bytes
	cut           = "cut"            // the file ends inside the record
)

// crashLayout is a log left by a crash: stretches of records whose durability
// Open knows, from a checkpoint or a seal, then the unknown tail.
type crashLayout struct {
	cfg       Config
	build     func(t *testing.T, dir string) *Log // returns the log still open
	durable   [][2]uint64                         // stretches Open trusts, in order
	unknown   [2]uint64                           // stretch past them, if any
	committed uint64
}

// crashCase damages records of a layout and appends tail bytes after them.
type crashCase struct {
	layout  string
	damages map[uint64]string
	tail    func(next uint64) []byte
}

var crashLayouts = map[string]crashLayout{
	"no checkpoint": {
		cfg: Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize},
		build: func(t *testing.T, dir string) *Log {
			l := openLog(t, dir, Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize})
			appendN(t, l, 6)
			require.NoError(t, l.Sync())

			return l
		},
		unknown: [2]uint64{1, 6},
	},
	"checkpoint in active": {
		cfg: Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize},
		build: func(t *testing.T, dir string) *Log {
			l := openLog(t, dir, Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize})
			appendN(t, l, 3)
			require.NoError(t, l.Commit(2))
			appendN(t, l, 3)
			require.NoError(t, l.Sync())

			return l
		},
		durable:   [][2]uint64{{1, 3}},
		unknown:   [2]uint64{4, 6},
		committed: 2,
	},
	"appended after close": {
		cfg: Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize},
		build: func(t *testing.T, dir string) *Log {
			cfg := Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize}

			l := openLog(t, dir, cfg)
			appendN(t, l, 6)
			require.NoError(t, l.Close())

			l = openLog(t, dir, cfg)
			appendN(t, l, 3)
			require.NoError(t, l.Sync())

			return l
		},
		durable: [][2]uint64{{1, 6}},
		unknown: [2]uint64{7, 9},
	},
	"rolled after checkpoint": {
		cfg: Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize},
		build: func(t *testing.T, dir string) *Log {
			l := openLog(t, dir, Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize})
			appendN(t, l, 2)
			require.NoError(t, l.Commit(1))
			appendN(t, l, 8)
			require.NoError(t, l.Sync())

			return l
		},
		durable:   [][2]uint64{{1, 4}, {5, 8}},
		unknown:   [2]uint64{9, 10},
		committed: 1,
	},
	"closed": {
		cfg: Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize},
		build: func(t *testing.T, dir string) *Log {
			l := openLog(t, dir, Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize})
			appendN(t, l, 6)
			require.NoError(t, l.Close())

			return l
		},
		durable: [][2]uint64{{1, 6}},
	},
}

// Open recovers a crashed log like PostgreSQL crash recovery: records past the
// checkpoint end at the first bad one, as a crash may leave any damage there,
// and everything after it is cut. Records a checkpoint or a seal made durable
// are never cut; Read reports their damage.
func TestCrash_Recovery(t *testing.T) {
	t.Parallel()

	gapThenValid := func(next uint64) []byte {
		return appendRecord(make([]byte, testRecordSize), next+1, payload(next+1))
	}
	zeros := func(uint64) []byte { return make([]byte, 4096) }
	junk := func(uint64) []byte { return garbageBytes(100) }

	cases := map[string]crashCase{
		"no checkpoint, intact":                  {layout: "no checkpoint"},
		"no checkpoint, the example pattern":     {layout: "no checkpoint", damages: map[uint64]string{4: badData, 6: badHeader}},
		"no checkpoint, first record bad":        {layout: "no checkpoint", damages: map[uint64]string{1: badHeader}},
		"no checkpoint, last record cut":         {layout: "no checkpoint", damages: map[uint64]string{6: cut}},
		"no checkpoint, partial header write":    {layout: "no checkpoint", damages: map[uint64]string{4: partialHeader}},
		"no checkpoint, partial data write":      {layout: "no checkpoint", damages: map[uint64]string{4: partialData}},
		"no checkpoint, unwritten page":          {layout: "no checkpoint", damages: map[uint64]string{3: zeroed}},
		"no checkpoint, stale record":            {layout: "no checkpoint", damages: map[uint64]string{5: stale}},
		"no checkpoint, garbage record":          {layout: "no checkpoint", damages: map[uint64]string{2: garbage}},
		"no checkpoint, every record bad":        {layout: "no checkpoint", damages: everyRecord(1, 6, badData)},
		"no checkpoint, zeros after":             {layout: "no checkpoint", tail: zeros},
		"no checkpoint, garbage after":           {layout: "no checkpoint", tail: junk},
		"no checkpoint, record after a gap":      {layout: "no checkpoint", tail: gapThenValid},
		"checkpoint in active, intact":           {layout: "checkpoint in active"},
		"checkpoint in active, example pattern":  {layout: "checkpoint in active", damages: map[uint64]string{4: badData, 6: badData}},
		"checkpoint in active, durable data":     {layout: "checkpoint in active", damages: map[uint64]string{2: badData}},
		"checkpoint in active, durable header":   {layout: "checkpoint in active", damages: map[uint64]string{2: badHeader}},
		"checkpoint in active, both regions":     {layout: "checkpoint in active", damages: map[uint64]string{2: badData, 5: stale}},
		"checkpoint in active, first past it":    {layout: "checkpoint in active", damages: map[uint64]string{4: partialData}},
		"checkpoint in active, zeroed durable":   {layout: "checkpoint in active", damages: map[uint64]string{3: zeroed}},
		"checkpoint in active, record after gap": {layout: "checkpoint in active", tail: gapThenValid},
		"after close, example pattern":           {layout: "appended after close", damages: map[uint64]string{7: badData, 9: badHeader}},
		"after close, durable and past":          {layout: "appended after close", damages: map[uint64]string{3: badData, 8: garbage}},
		"after close, last record cut":           {layout: "appended after close", damages: map[uint64]string{9: cut}},
		"rolled, intact":                         {layout: "rolled after checkpoint"},
		"rolled, example pattern":                {layout: "rolled after checkpoint", damages: map[uint64]string{8: badData, 10: badData}},
		"rolled, sealed header":                  {layout: "rolled after checkpoint", damages: map[uint64]string{6: badHeader}},
		"rolled, active emptied":                 {layout: "rolled after checkpoint", damages: map[uint64]string{9: zeroed}},
		"rolled, damage everywhere":              {layout: "rolled after checkpoint", damages: map[uint64]string{2: badData, 5: stale, 9: partialHeader}},
		"closed, example pattern":                {layout: "closed", damages: map[uint64]string{4: badData, 6: badHeader}},
		"closed, every record bad":               {layout: "closed", damages: everyRecord(1, 6, badData)},
		"closed, zeros after":                    {layout: "closed", tail: zeros},
	}

	for name, tc := range cases {
		for mode, indexed := range indexModes {
			t.Run(name+"/"+mode, func(t *testing.T) {
				t.Parallel()
				testCrashRecovery(t, crashLayouts[tc.layout], tc, indexed)
			})
		}
	}
}

func testCrashRecovery(t *testing.T, layout crashLayout, tc crashCase, indexed bool) {
	dir := t.TempDir()
	l := layout.build(t, dir)
	last, size := l.LastIndex(), l.Size()

	crashed := snapshot(t, dir)
	if !indexed {
		removeIndexes(t, crashed)
	}

	for index, kind := range tc.damages {
		damageRecord(t, crashed, layout, index, kind)
	}

	if tc.tail != nil {
		appendBytes(t, activePath(t, crashed), tc.tail(last+1))
	}

	want := wantRecovery(layout, tc, last)

	l = openLog(t, crashed, layout.cfg)
	require.Equal(t, want.last, l.LastIndex())
	require.Equal(t, layout.committed, l.Committed())
	require.Equal(t, size-int64(last-want.last)*testRecordSize, l.Size(), "the cut bytes are gone")
	requireRecovered(t, l, want)

	// A crash right after Open recovers the same log.
	again := openLog(t, snapshot(t, crashed), layout.cfg)
	require.Equal(t, want.last, again.LastIndex())
	requireRecovered(t, again, want)

	// A new record takes the first cut index, and no cut record after it comes
	// back, even one that was intact.
	index, err := l.Append(renewed(want.last + 1))
	require.NoError(t, err)
	require.Equal(t, want.last+1, index)
	require.NoError(t, l.Sync())

	l = openLog(t, snapshot(t, crashed), layout.cfg)
	require.Equal(t, want.last+1, l.LastIndex())
	requireRecovered(t, l, want)

	recs, err := l.Read(want.last+1, 10)
	require.NoError(t, err)
	require.Equal(t, []Record{{Index: want.last + 1, Data: renewed(want.last + 1)}}, recs)
}

// recovery is what Open should make of a crashed log: its last index and the
// ranges of durable records that Read reports lost.
type recovery struct {
	last uint64
	lost [][2]uint64
}

func wantRecovery(layout crashLayout, tc crashCase, last uint64) recovery {
	want := recovery{last: last}

	if layout.unknown != [2]uint64{} {
		for index := layout.unknown[0]; index <= layout.unknown[1]; index++ {
			if _, bad := tc.damages[index]; bad {
				want.last = index - 1

				break
			}
		}
	}

	// A bad header loses the rest of its stretch, other damage one record.
	for _, stretch := range layout.durable {
		for index := stretch[0]; index <= stretch[1]; index++ {
			switch tc.damages[index] {
			case "":
			case badData, stale, partialData:
				want.lost = append(want.lost, [2]uint64{index, index})
			default:
				want.lost = append(want.lost, [2]uint64{index, stretch[1]})
				index = stretch[1]
			}
		}
	}

	return want
}

// requireRecovered checks every record up to want.last: intact ones read back,
// lost ones fail with the range they belong to.
func requireRecovered(t *testing.T, l *Log, want recovery) {
	t.Helper()

	for index := uint64(1); index <= want.last; index++ {
		recs, err := l.Read(index, 1)

		lost := lostRangeOf(want.lost, index)
		if lost == nil {
			require.NoError(t, err, "record %d", index)
			require.Equal(t, payload(index), recs[0].Data, "record %d", index)

			continue
		}

		var corrupt *CorruptError
		require.ErrorAs(t, err, &corrupt, "record %d", index)
		require.Equal(t, lost[0], corrupt.First, "record %d", index)
		require.Equal(t, lost[1], corrupt.Last, "record %d", index)
	}
}

func lostRangeOf(ranges [][2]uint64, index uint64) *[2]uint64 {
	for i := range ranges {
		if ranges[i][0] <= index && index <= ranges[i][1] {
			return &ranges[i]
		}
	}

	return nil
}

// damageRecord applies kind to record index of the layout's files in dir.
func damageRecord(t *testing.T, dir string, layout crashLayout, index uint64, kind string) {
	t.Helper()

	first := segmentFirst(layout, index)
	path := filepath.Join(dir, segmentName(first))
	offset := segmentHeaderSize + int64(index-first)*testRecordSize

	switch kind {
	case badData:
		flipByte(t, path, offset+recordHeaderSize+1)
	case badHeader:
		flipByte(t, path, offset+1)
	case zeroed:
		zeroRange(t, path, offset, offset+testRecordSize)
	case partialHeader:
		zeroRange(t, path, offset+5, offset+testRecordSize)
	case partialData:
		zeroRange(t, path, offset+recordHeaderSize+3, offset+testRecordSize)
	case stale:
		writeAt(t, path, offset, appendRecord(nil, index+100, payload(index)))
	case garbage:
		writeAt(t, path, offset, garbageBytes(testRecordSize))
	case cut:
		require.NoError(t, os.Truncate(path, offset+recordHeaderSize/2))
	default:
		t.Fatalf("unknown damage %q", kind)
	}
}

// segmentFirst returns the first index of the segment holding index.
func segmentFirst(layout crashLayout, index uint64) uint64 {
	per := uint64((layout.cfg.SegmentSize - segmentHeaderSize) / testRecordSize)

	return (index-1)/per*per + 1
}

// removeIndexes deletes the index files in dir, so Open scans every segment.
func removeIndexes(t *testing.T, dir string) {
	t.Helper()

	for _, name := range indexFiles(t, dir) {
		require.NoError(t, os.Remove(filepath.Join(dir, name)))
	}
}

// activePath returns the path of the last segment in dir.
func activePath(t *testing.T, dir string) string {
	t.Helper()

	names := segmentFiles(t, dir)

	return filepath.Join(dir, names[len(names)-1])
}

func everyRecord(first, last uint64, kind string) map[uint64]string {
	damages := make(map[uint64]string)
	for index := first; index <= last; index++ {
		damages[index] = kind
	}

	return damages
}

// renewed is the payload of a record appended after recovery, distinct from the
// one a cut record had at that index.
func renewed(index uint64) []byte {
	return fmt.Appendf(nil, "renew-%06d", index)
}

// garbageBytes returns n bytes that are never zero and never a valid record.
func garbageBytes(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i*37 + 11)
	}

	return buf
}

func writeAt(t *testing.T, path string, offset int64, data []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = file.WriteAt(data, offset)
	require.NoError(t, errors.Join(err, file.Close()))
}

// An index written by a Close whose checkpoint failed can describe records Open
// cuts. New records that fill the same bytes and indexes must not reuse it.
func TestCrash_CutDropsStaleIndex(t *testing.T) {
	t.Parallel()

	dir, cfg := staleIndexLog(t)

	l := openLog(t, dir, cfg)
	require.Equal(t, uint64(4), l.LastIndex())
	require.NoFileExists(t, filepath.Join(dir, indexName(1)))

	refillStaleIndex(t, l, dir, cfg)
}

// Open deletes the index before it cuts. When the cut fails, as a crash there
// would stop it, the next Open cuts without the index.
func TestCrash_OpenStoppedAfterIndexRemoval(t *testing.T) {
	dir, cfg := staleIndexLog(t)
	path := filepath.Join(dir, segmentName(1))

	info, err := os.Stat(path)
	require.NoError(t, err)

	injected := errors.New("injected truncate failure")
	truncateFile = func(*os.File, int64) error { return injected }

	_, err = Open(dir, cfg)
	truncateFile = (*os.File).Truncate

	require.ErrorIs(t, err, injected)
	require.NoFileExists(t, filepath.Join(dir, indexName(1)), "the index went first")

	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, info.Size(), after.Size(), "nothing was cut")

	l := openLog(t, dir, cfg)
	require.Equal(t, uint64(4), l.LastIndex())

	refillStaleIndex(t, l, dir, cfg)
}

// staleIndexLog leaves a log whose Close wrote an index and then failed to write
// its checkpoint, with record 5, past the older checkpoint, damaged.
func staleIndexLog(t *testing.T) (string, Config) {
	t.Helper()

	dir := t.TempDir()
	cfg := Config{SegmentSize: 1 << 20, MaxRecordSize: 2000}

	l := openLog(t, dir, cfg)
	for range 4 {
		_, err := l.Append(bytes.Repeat([]byte("x"), 1000))
		require.NoError(t, err)
	}

	require.NoError(t, l.Commit(1))

	for range 8 {
		_, err := l.Append(bytes.Repeat([]byte("x"), 1000))
		require.NoError(t, err)
	}

	unblock := blockPath(t, filepath.Join(dir, checkpointName+tempExt))
	require.Error(t, l.Close())
	unblock()
	require.FileExists(t, filepath.Join(dir, indexName(1)))

	flipByte(t, filepath.Join(dir, segmentName(1)), segmentHeaderSize+4*1012+recordHeaderSize)

	return dir, cfg
}

// refillStaleIndex appends records 5 to 12 into the bytes of the cut ones, with
// 5 and 6 split differently, commits and checks that a crash reads all 12.
func refillStaleIndex(t *testing.T, l *Log, dir string, cfg Config) {
	t.Helper()

	for _, n := range []int{500, 1500, 1000, 1000, 1000, 1000, 1000, 1000} {
		_, err := l.Append(bytes.Repeat([]byte("x"), n))
		require.NoError(t, err)
	}

	require.NoError(t, l.Commit(12))

	l = openLog(t, snapshot(t, dir), cfg)
	for index := uint64(1); index <= 12; index++ {
		_, err := l.Read(index, 1)
		require.NoError(t, err, "record %d", index)
	}
}
