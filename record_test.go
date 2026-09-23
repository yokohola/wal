package wal

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"empty": {},
		"small": []byte("hello"),
		"large": bytes.Repeat([]byte("x"), 100<<10),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			buf := appendRecord([]byte("prefix"), data)
			require.Len(t, buf, len("prefix")+recordHeaderSize+len(data))

			got, err := decodeRecord(buf[len("prefix"):])
			require.NoError(t, err)
			require.Equal(t, data, got)
		})
	}
}

func TestRecord_DecodeReportsFailureKind(t *testing.T) {
	t.Parallel()

	valid := appendRecord(nil, []byte("payload"))

	cases := map[string]struct {
		buf  []byte
		want error
	}{
		"empty":          {buf: nil, want: errShortRecord},
		"short header":   {buf: valid[:recordHeaderSize-1], want: errShortRecord},
		"short data":     {buf: valid[:len(valid)-1], want: errShortRecord},
		"zero header":    {buf: make([]byte, 64), want: errBadHeader},
		"flipped length": {buf: flipped(valid, 0), want: errBadHeader},
		"flipped crc":    {buf: flipped(valid, 5), want: errBadHeader},
		"flipped data":   {buf: flipped(valid, len(valid)-1), want: errBadData},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeRecord(tc.buf)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestRecord_EmptyRecordDiffersFromZeroFill(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, make([]byte, recordHeaderSize), appendRecord(nil, nil))
}

func TestRecord_DecodedDataDoesNotReachNextRecord(t *testing.T) {
	t.Parallel()

	buf := appendRecord(appendRecord(nil, []byte("first")), []byte("second"))

	first, err := decodeRecord(buf)
	require.NoError(t, err)
	require.Equal(t, len(first), cap(first))

	_ = append(first, "XXXXXXXXXXXXXXXXXXXX"...)

	second, err := decodeRecord(buf[recordHeaderSize+len("first"):])
	require.NoError(t, err)
	require.Equal(t, []byte("second"), second)
}

func TestRecordReader_CrossesChunks(t *testing.T) {
	t.Parallel()

	sizes := []int{0, 1, readChunkSize - recordHeaderSize - 3, 7, readChunkSize + 11, 5, 2 * readChunkSize}

	var (
		buf  []byte
		want [][]byte
	)

	for i, size := range sizes {
		data := bytes.Repeat([]byte{byte('a' + i)}, size)
		want = append(want, data)
		buf = appendRecord(buf, data)
	}

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}

	var got [][]byte

	for {
		data, err := reader.next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		got = append(got, data)
	}

	require.Equal(t, want, got)
	require.Equal(t, int64(len(buf)), reader.offset)
}

func TestRecordReader_StopsAtFailingRecord(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, []byte("good"))
	failAt := int64(len(buf))
	buf = appendRecord(buf, []byte("bad"))
	buf[len(buf)-1] ^= 0xff

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}

	_, err := reader.next()
	require.NoError(t, err)

	_, err = reader.next()
	require.ErrorIs(t, err, errBadData)
	require.Equal(t, failAt, reader.offset)
}

func TestRecordReader_FileShorterThanEnd(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, []byte("payload"))
	reader := recordReader{src: bytes.NewReader(buf[:len(buf)-2]), end: int64(len(buf))}

	_, err := reader.next()
	require.ErrorIs(t, err, errShortRecord)
}

func TestHasZeroSector(t *testing.T) {
	t.Parallel()

	ones := func(n int) []byte { return bytes.Repeat([]byte{1}, n) }

	cases := map[string]struct {
		buf    []byte
		offset int64
		want   bool
	}{
		"no zeros":                 {buf: ones(2000), offset: 100, want: false},
		"zeros not filling sector": {buf: append(ones(10), make([]byte, 100)...), offset: 0, want: false},
		"whole zero sector":        {buf: append(append(ones(412), make([]byte, 512)...), ones(10)...), offset: 100, want: true},
		"zero head up to boundary": {buf: append(make([]byte, 12), ones(600)...), offset: 500, want: true},
		"zero tail from boundary":  {buf: append(ones(12), make([]byte, 4)...), offset: 500, want: true},
		"all zero":                 {buf: make([]byte, 12), offset: 7, want: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, hasZeroSector(tc.buf, tc.offset))
		})
	}
}

func TestSegmentName_RoundTrip(t *testing.T) {
	t.Parallel()

	require.Equal(t, "00000000000000000001.wal", segmentName(1))
	require.Equal(t, "18446744073709551615.wal", segmentName(^uint64(0)))

	for _, first := range []uint64{1, 42, ^uint64(0)} {
		got, ok := parseSegmentName(segmentName(first))
		require.True(t, ok)
		require.Equal(t, first, got)
	}

	for _, name := range []string{
		"1.wal", "checkpoint", "LOCK", "00000000000000000001.wal.tmp",
		"0000000000000000000a.wal", "00000000000000000000.wal", "99999999999999999999.wal",
	} {
		_, ok := parseSegmentName(name)
		require.False(t, ok, name)
	}
}

func TestIsTempName(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"00000000000000000001.wal.tmp", "checkpoint.tmp"} {
		require.True(t, isTempName(name), name)
	}

	for _, name := range []string{"notes.tmp", "1.wal.tmp", "checkpoint", "00000000000000000001.wal", "LOCK.tmp"} {
		require.False(t, isTempName(name), name)
	}
}

func TestCheckpoint_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cp, found, err := readCheckpoint(dir)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, cp)

	want := checkpoint{committed: 43, segment: 40, end: 1 << 40, next: math.MaxUint64}

	require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 42}))
	require.NoError(t, writeCheckpoint(dir, want))

	cp, found, err = readCheckpoint(dir)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want, cp)
	require.NoFileExists(t, filepath.Join(dir, checkpointName+tempExt))
}

func TestCheckpoint_RejectsDamage(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T, path string){
		"short": func(t *testing.T, path string) {
			t.Helper()
			require.NoError(t, os.WriteFile(path, []byte("short"), 0o644))
		},
		"oversized": func(t *testing.T, path string) {
			t.Helper()
			require.NoError(t, os.WriteFile(path, make([]byte, 1<<20), 0o644))
		},
		"flipped": func(t *testing.T, path string) {
			t.Helper()
			flipByte(t, path, 3)
		},
	}

	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 7}))
			damage(t, filepath.Join(dir, checkpointName))

			_, _, err := readCheckpoint(dir)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func flipped(buf []byte, offset int) []byte {
	out := bytes.Clone(buf)
	out[offset] ^= 0xff

	return out
}
