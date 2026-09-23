package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The index of a segment starting at 100 whose records below 110 end at 256.
const (
	testIndexFirst = 100
	testIndexEnd   = 256
	testIndexNext  = 110
)

var testIndexEntries = []indexEntry{{index: 100, offset: segmentHeaderSize}, {index: 105, offset: 136}}

func TestIndex_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		first, next uint64
		end         int64
		entries     []indexEntry
	}{
		"sparse": {first: testIndexFirst, end: testIndexEnd, next: testIndexNext, entries: testIndexEntries},
		"one entry": {
			first: 1, end: segmentOf(4), next: 5,
			entries: []indexEntry{{index: 1, offset: segmentHeaderSize}},
		},
		// Every record is empty and indexed, and the last one ends at end.
		"densest": {
			first: 7, end: segmentHeaderSize + 3*recordHeaderSize, next: 10,
			entries: []indexEntry{
				{index: 7, offset: segmentHeaderSize},
				{index: 8, offset: segmentHeaderSize + recordHeaderSize},
				{index: 9, offset: segmentHeaderSize + 2*recordHeaderSize},
			},
		},
		"largest values": {
			first: 1 << 62, end: 1 << 62, next: 1<<62 + 1,
			entries: []indexEntry{{index: 1 << 62, offset: segmentHeaderSize}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			buf := encodeIndex(tc.first, tc.end, tc.next, tc.entries)
			require.Len(t, buf, indexHeaderSize+len(tc.entries)*indexEntrySize+indexCRCSize)

			got, err := decodeIndex(buf, tc.first, tc.end, tc.next)
			require.NoError(t, err)
			require.Equal(t, tc.entries, got)
		})
	}
}

// Each case breaks one rule and keeps the others, so it is rejected by the
// check for that rule alone.
func TestIndex_DecodeRejects(t *testing.T) {
	t.Parallel()

	valid := encodeIndex(testIndexFirst, testIndexEnd, testIndexNext, testIndexEntries)

	// reseal replaces the checksum of buf after an edit to its body.
	reseal := func(buf []byte) []byte {
		body := buf[:len(buf)-indexCRCSize]

		return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, crcTable))
	}

	withBody := func(edit func(body []byte) []byte) []byte {
		body := append([]byte(nil), valid[:len(valid)-indexCRCSize]...)

		return reseal(append(edit(body), 0, 0, 0, 0))
	}

	withEntries := func(entries ...indexEntry) []byte {
		return encodeIndex(testIndexFirst, testIndexEnd, testIndexNext, entries)
	}

	cases := map[string]struct {
		buf    []byte
		reason string
	}{
		"empty":        {buf: nil, reason: "0 bytes"},
		"header only":  {buf: valid[:indexHeaderSize], reason: "32 bytes"},
		"no entries":   {buf: withEntries(), reason: "36 bytes"},
		"torn entry":   {buf: withBody(func(body []byte) []byte { return body[:len(body)-1] }), reason: "67 bytes"},
		"extra byte":   {buf: withBody(func(body []byte) []byte { return append(body, 0) }), reason: "69 bytes"},
		"bad checksum": {buf: flipped(valid, len(valid)-1), reason: "checksum mismatch"},
		"bad body":     {buf: flipped(valid, indexHeaderSize), reason: "checksum mismatch"},
		"bad magic": {
			buf: withBody(func(body []byte) []byte { body[0] = 'X'; return body }), reason: "bad header",
		},
		"bad version": {
			buf: withBody(func(body []byte) []byte { body[4] = indexVersion + 1; return body }), reason: "bad header",
		},
		"other first": {
			buf:    encodeIndex(testIndexFirst+1, testIndexEnd, testIndexNext, testIndexEntries),
			reason: "records 101 to 110 ending at 256, want 100 to 110 ending at 256",
		},
		"other end": {
			buf:    encodeIndex(testIndexFirst, testIndexEnd+recordHeaderSize, testIndexNext, testIndexEntries),
			reason: "ending at 268, want",
		},
		"other next": {
			buf:    encodeIndex(testIndexFirst, testIndexEnd, testIndexNext-1, testIndexEntries),
			reason: "records 100 to 109",
		},
		"first entry past first record": {
			buf: withEntries(indexEntry{index: 101, offset: segmentHeaderSize}, testIndexEntries[1]), reason: "first entry",
		},
		"first entry past segment header": {
			buf: withEntries(indexEntry{index: 100, offset: 40}, testIndexEntries[1]), reason: "first entry",
		},
		"entry repeats an index": {
			buf: withEntries(testIndexEntries[0], indexEntry{index: 100, offset: 136}), reason: "entry 100 at 136",
		},
		"entry offset goes back": {
			buf: withEntries(testIndexEntries[0], indexEntry{index: 101, offset: 4}), reason: "entry 101 at 4",
		},
		"entries too close": {
			buf:    withEntries(testIndexEntries[0], indexEntry{index: 105, offset: 16 + 5*recordHeaderSize - 1}),
			reason: "entry 105 at 75",
		},
		"last entry at next": {
			buf: withEntries(testIndexEntries[0], indexEntry{index: 110, offset: 136}), reason: "last entry 110",
		},
		"last entry too close to end": {
			buf:    withEntries(testIndexEntries[0], indexEntry{index: 105, offset: testIndexEnd - 5*recordHeaderSize + 1}),
			reason: "last entry 105 at 197",
		},
		"last entry past end": {
			buf:    withEntries(testIndexEntries[0], indexEntry{index: 105, offset: testIndexEnd + 1}),
			reason: "last entry 105 at 257",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeIndex(tc.buf, testIndexFirst, testIndexEnd, testIndexNext)
			require.ErrorIs(t, err, errBadIndex)
			require.ErrorContains(t, err, tc.reason)
		})
	}
}

// The tightest layout the rules allow decodes, so the checks are not stricter
// than a record header per record.
func TestIndex_DecodeAcceptsTightestLayout(t *testing.T) {
	t.Parallel()

	entries := []indexEntry{
		{index: 100, offset: segmentHeaderSize},
		{index: 105, offset: segmentHeaderSize + 5*recordHeaderSize},
	}
	end := int64(segmentHeaderSize + 10*recordHeaderSize)

	got, err := decodeIndex(encodeIndex(100, end, 110, entries), 100, end, 110)
	require.NoError(t, err)
	require.Equal(t, entries, got)
}

func TestIndex_EveryFlippedBitIsRejected(t *testing.T) {
	t.Parallel()

	valid := encodeIndex(testIndexFirst, testIndexEnd, testIndexNext, testIndexEntries)

	for bit := range len(valid) * 8 {
		buf := append([]byte(nil), valid...)
		buf[bit/8] ^= 1 << (bit % 8)

		_, err := decodeIndex(buf, testIndexFirst, testIndexEnd, testIndexNext)
		require.ErrorIs(t, err, errBadIndex, "bit %d", bit)
	}
}

func TestReadIndex(t *testing.T) {
	t.Parallel()

	// A segment of 4 empty records has room for at most 4 entries.
	end := int64(segmentHeaderSize + 4*recordHeaderSize)

	var densest []indexEntry
	for i := range int64(4) {
		densest = append(densest, indexEntry{index: 1 + uint64(i), offset: segmentHeaderSize + i*recordHeaderSize})
	}

	t.Run("densest", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), indexName(1))
		require.NoError(t, os.WriteFile(path, encodeIndex(1, end, 5, densest), 0o644))

		got, err := readIndex(path, 1, end, 5)
		require.NoError(t, err)
		require.Equal(t, densest, got)
	})

	t.Run("more entries than records fit", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), indexName(1))
		require.NoError(t, os.WriteFile(path, encodeIndex(1, end, 5, append(densest, densest[3])), 0o644))

		_, err := readIndex(path, 1, end, 5)
		require.ErrorIs(t, err, errBadIndex)
		require.ErrorContains(t, err, "has 116 bytes")
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()

		_, err := readIndex(filepath.Join(t.TempDir(), indexName(1)), 1, end, 5)
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("a directory", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), indexName(1))
		require.NoError(t, os.Mkdir(path, 0o755))

		_, err := readIndex(path, 1, end, 5)
		require.Error(t, err)
	})
}

func TestWriteIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, indexName(1))

	for _, entries := range [][]indexEntry{testIndexEntries, testIndexEntries[:1]} {
		buf := encodeIndex(testIndexFirst, testIndexEnd, testIndexNext, entries)
		require.NoError(t, writeIndex(path, buf))

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, buf, got, "replaced")
	}

	require.NoFileExists(t, path+tempExt)

	// A rename cannot replace a non-empty directory.
	blocked := filepath.Join(dir, indexName(2))
	require.NoError(t, os.Mkdir(blocked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blocked, "file"), nil, 0o644))

	require.Error(t, writeIndex(blocked, []byte("index")))
	require.NoFileExists(t, blocked+tempExt)
}

func TestIndexName_RoundTrip(t *testing.T) {
	t.Parallel()

	require.Equal(t, "00000000000000000001.idx", indexName(1))

	for _, first := range []uint64{1, 42, ^uint64(0)} {
		got, ok := parseIndexName(indexName(first))
		require.True(t, ok)
		require.Equal(t, first, got)

		_, ok = parseSegmentName(indexName(first))
		require.False(t, ok)

		_, ok = parseIndexName(segmentName(first))
		require.False(t, ok)
	}

	for _, name := range []string{"1.idx", "00000000000000000000.idx", "00000000000000000001.idx.tmp"} {
		_, ok := parseIndexName(name)
		require.False(t, ok, name)
	}
}

// Rolling seals a segment and writes its index; Close does so for the active
// one. The indexes hold exactly the in-memory sparse index and add nothing to
// Size.
func TestIndex_WrittenWhenSegmentsSeal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(400), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 1000)

	requireIndexOf := func(seg *segment) {
		t.Helper()

		got, err := readIndex(seg.indexPath(), seg.first, seg.size, seg.nextIndex())
		require.NoError(t, err)
		require.Equal(t, seg.sparseIndex, got)
	}

	require.Len(t, l.segments, 3)
	requireIndexOf(l.segments[0])
	requireIndexOf(l.segments[1])
	require.NoFileExists(t, l.segments[2].indexPath(), "the active segment is not sealed")
	require.Equal(t, []string{indexName(1), indexName(401)}, indexFiles(t, dir))

	var segmentBytes int64

	for _, seg := range l.segments {
		segmentBytes += seg.size
	}

	require.Equal(t, segmentBytes, l.Size())

	active := l.segments[2]
	require.NoError(t, l.Close())
	requireIndexOf(active)

	l = openLog(t, dir, cfg)
	require.Equal(t, segmentBytes, l.Size(), "index files are not counted")
	requireRecords(t, readAll(t, l), 1, 1000)

	// The active segment loaded its index; records appended since are indexed
	// by the next Close as well.
	active = l.segments[2]
	appendN(t, l, 100)
	require.Len(t, l.segments, 3)
	require.NoError(t, l.Close())
	requireIndexOf(active)
}

func TestIndex_NotWrittenForEmptySegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{})
	require.NoError(t, l.Close())
	require.Empty(t, indexFiles(t, dir))
}

// An index is written only for a segment whose records are all located.
func TestIndex_NotWrittenUnlessAllRecordsLocated(t *testing.T) {
	t.Parallel()

	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}

	// closedLog holds records 1 to 3 in segment 1, which a Close indexed.
	closedLog := func(t *testing.T) (string, []byte) {
		t.Helper()

		dir := t.TempDir()
		l := openLog(t, dir, cfg)
		appendN(t, l, 3)
		require.NoError(t, l.Close())

		old, err := os.ReadFile(filepath.Join(dir, indexName(1)))
		require.NoError(t, err)

		return dir, old
	}

	t.Run("extended after Open", func(t *testing.T) {
		t.Parallel()

		dir, old := closedLog(t)

		// Open loads the index; the roll rewrites it with the appended record.
		l := openLog(t, dir, cfg)
		appendN(t, l, 2)
		require.Len(t, l.segments, 2)

		got, err := os.ReadFile(filepath.Join(dir, indexName(1)))
		require.NoError(t, err)
		require.NotEqual(t, old, got)

		_, err = readIndex(filepath.Join(dir, indexName(1)), 1, segmentOf(4), 5)
		require.NoError(t, err)

		require.NoError(t, l.Close())

		l = openLog(t, dir, cfg)
		require.Empty(t, lostIndexes(t, l))
		requireRecords(t, readAll(t, l), 1, 5)
	})

	t.Run("unlocated when rolled", func(t *testing.T) {
		t.Parallel()

		dir, _ := closedLog(t)
		require.NoError(t, os.Remove(filepath.Join(dir, indexName(1))))
		flipByte(t, filepath.Join(dir, segmentName(1)), segmentOf(1))

		l := openLog(t, dir, cfg)
		require.Equal(t, []uint64{2, 3}, lostIndexes(t, l))

		appendN(t, l, 2)
		require.Len(t, l.segments, 2)
		require.NoFileExists(t, filepath.Join(dir, indexName(1)))

		require.NoError(t, l.Close())

		l = openLog(t, dir, cfg)
		require.NoFileExists(t, filepath.Join(dir, indexName(1)), "Open writes no index for damage")
		require.Equal(t, []uint64{2, 3, 4}, lostIndexes(t, l), "segment 1 is scanned")
	})

	t.Run("more trusted than on disk", func(t *testing.T) {
		t.Parallel()

		// The checkpoint claims five records where four are, so verification
		// reaches the end of the trusted bytes and still misses one.
		dir := t.TempDir()
		writeSegment(t, dir, 1, payload(1), payload(2), payload(3), payload(4))
		require.NoError(t, writeCheckpoint(dir, checkpoint{segment: 1, end: segmentOf(4), next: 6}))

		l := openLog(t, dir, cfg)
		require.Equal(t, []uint64{5}, lostIndexes(t, l))
		require.Equal(t, l.segments[0].trustedEnd, l.segments[0].readableEnd)

		require.NoError(t, l.Close())
		require.NoFileExists(t, filepath.Join(dir, indexName(1)))
	})

	t.Run("fewer located than trusted", func(t *testing.T) {
		t.Parallel()

		// The checkpoint claims three records, but four fill its bytes.
		dir := t.TempDir()
		writeSegment(t, dir, 1, payload(1), payload(2), payload(3), payload(4))
		require.NoError(t, writeCheckpoint(dir, checkpoint{segment: 1, end: segmentOf(4), next: 4}))

		l := openLog(t, dir, cfg)
		require.Empty(t, lostIndexes(t, l))
		require.NotEqual(t, l.segments[0].trustedEnd, l.segments[0].readableEnd)

		require.NoError(t, l.Close())
		require.NoFileExists(t, filepath.Join(dir, indexName(1)))
	})
}

// The index is a cache, so failing to write it fails nothing.
func TestIndex_WriteFailureIsIgnored(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)

	// A non-empty directory in place of the index of segment 1 fails its rename.
	blocked := filepath.Join(dir, indexName(1))
	require.NoError(t, os.Mkdir(blocked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blocked, "file"), nil, 0o644))

	appendN(t, l, 10)
	require.NoFileExists(t, blocked+tempExt)
	require.FileExists(t, filepath.Join(dir, indexName(5)))
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	require.Empty(t, lostIndexes(t, l))
	requireRecords(t, readAll(t, l), 1, 10)
}

// Open scans a closed segment whose index is unusable and writes the index at
// once, the same one a roll writes, so the next Open loads it. The active
// segment's index waits for its seal.
func TestReopen_RebuildsIndexes(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T, path string){
		"missing": func(t *testing.T, path string) {
			t.Helper()
			require.NoError(t, os.Remove(path))
		},
		"torn": func(t *testing.T, path string) {
			t.Helper()
			require.NoError(t, os.Truncate(path, indexHeaderSize))
		},
		"stale": func(t *testing.T, path string) {
			t.Helper()

			stale := encodeIndex(1, segmentOf(399), 400, []indexEntry{{index: 1, offset: segmentHeaderSize}})
			require.NoError(t, os.WriteFile(path, stale, 0o644))
		},
	}

	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := writeBigLog(t, true)

			want := map[uint64][]byte{}

			for _, first := range []uint64{1, 401, 801} {
				data, err := os.ReadFile(filepath.Join(dir, indexName(first)))
				require.NoError(t, err)

				want[first] = data
				damage(t, filepath.Join(dir, indexName(first)))
			}

			l := openLog(t, dir, bigLogConfig)

			for _, first := range []uint64{1, 401} {
				got, err := os.ReadFile(filepath.Join(dir, indexName(first)))
				require.NoError(t, err)
				require.Equal(t, want[first], got, "segment %d", first)
			}

			require.NotEqual(t, want[801], readFileOrNil(t, filepath.Join(dir, indexName(801))),
				"the active segment is not sealed")

			require.Empty(t, lostIndexes(t, l))
			require.NoError(t, l.Close())
			useIndexes(t, dir, true, bigLogRecords)
		})
	}
}

// createSegment replaces an index left by an earlier segment of the same name.
func TestCreateSegment_RemovesStaleIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	stale := filepath.Join(dir, indexName(5))
	require.NoError(t, os.WriteFile(stale, encodeIndex(5, segmentOf(4), 9, []indexEntry{{5, segmentHeaderSize}}), 0o644))

	seg, err := createSegment(dir, 5, 0)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	require.NoFileExists(t, stale)
}

func indexFiles(t *testing.T, dir string) []string {
	t.Helper()

	names, err := filepath.Glob(filepath.Join(dir, "*"+indexExt))
	require.NoError(t, err)

	for i, name := range names {
		names[i] = filepath.Base(name)
	}

	return names
}

// readFileOrNil returns the contents of path, or nil when it does not exist.
func readFileOrNil(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	require.NoError(t, err)

	return data
}
