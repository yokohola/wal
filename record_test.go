package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// fetchLog records the length of every ReadAt.
type fetchLog struct {
	src     io.ReaderAt
	fetches []int
}

func (f *fetchLog) ReadAt(p []byte, off int64) (int, error) {
	f.fetches = append(f.fetches, len(p))

	return f.src.ReadAt(p, off)
}

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

			buf := appendRecord([]byte("prefix"), 7, data)
			require.Len(t, buf, len("prefix")+recordHeaderSize+len(data))

			got, err := decodeRecord(buf[len("prefix"):], 7)
			require.NoError(t, err)
			require.Equal(t, data, got)
		})
	}
}

func TestRecord_DecodeReportsFailureKind(t *testing.T) {
	t.Parallel()

	valid := appendRecord(nil, 1, []byte("payload"))

	cases := map[string]struct {
		buf   []byte
		index uint64
		want  error
	}{
		"empty":          {buf: nil, index: 1, want: errShortRecord},
		"short header":   {buf: valid[:recordHeaderSize-1], index: 1, want: errShortRecord},
		"short data":     {buf: valid[:len(valid)-1], index: 1, want: errShortRecord},
		"zero header":    {buf: make([]byte, 64), index: 1, want: errBadHeader},
		"flipped length": {buf: flipped(valid, 0), index: 1, want: errBadHeader},
		"flipped crc":    {buf: flipped(valid, 5), index: 1, want: errBadHeader},
		"flipped data":   {buf: flipped(valid, len(valid)-1), index: 1, want: errBadData},
		"other index":    {buf: valid, index: 2, want: errBadData},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeRecord(tc.buf, tc.index)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestRecord_DataChecksumCoversIndexThenData(t *testing.T) {
	t.Parallel()

	for _, index := range []uint64{0, 1, 0x0102030405060708, math.MaxUint64} {
		for _, data := range [][]byte{nil, []byte("payload")} {
			want := crc32.Checksum(binary.LittleEndian.AppendUint64(nil, index), crcTable)
			want = crc32.Update(want, crcTable, data)
			require.Equal(t, want, dataChecksum(index, data))
		}
	}
}

func TestRecord_EmptyRecordDiffersFromZeroFill(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, make([]byte, recordHeaderSize), appendRecord(nil, 1, nil))
}

func TestRecord_DecodedDataDoesNotReachNextRecord(t *testing.T) {
	t.Parallel()

	buf := appendRecord(appendRecord(nil, 1, []byte("first")), 2, []byte("second"))

	first, err := decodeRecord(buf, 1)
	require.NoError(t, err)
	require.Equal(t, len(first), cap(first))

	_ = append(first, "XXXXXXXXXXXXXXXXXXXX"...)

	second, err := decodeRecord(buf[recordHeaderSize+len("first"):], 2)
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
		buf = appendRecord(buf, uint64(i+1), data)
	}

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}

	var got [][]byte

	for index := uint64(1); ; index++ {
		data, err := reader.next(index)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		got = append(got, data)
	}

	require.Equal(t, want, got)
	require.Equal(t, int64(len(buf)), reader.offset)
}

func TestRecordReader_FetchesUpToStop(t *testing.T) {
	t.Parallel()

	var buf []byte
	for i := range uint64(4) {
		buf = appendRecord(buf, i+1, []byte("data"))
	}

	stop := int64(2 * (recordHeaderSize + len("data")))
	src := &fetchLog{src: bytes.NewReader(buf)}
	reader := recordReader{src: src, end: int64(len(buf)), stop: stop}

	for index := uint64(1); index <= 2; index++ {
		data, err := reader.next(index)
		require.NoError(t, err)
		require.Equal(t, []byte("data"), data)
	}

	require.Equal(t, []int{int(stop)}, src.fetches)

	// Past stop, each fetch takes only what the record needs.
	data, err := reader.next(3)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), data)
	require.Equal(t, []int{int(stop), recordHeaderSize, recordHeaderSize + len("data")}, src.fetches)
}

func TestRecordReader_RecordRunsPastStop(t *testing.T) {
	t.Parallel()

	large := bytes.Repeat([]byte("x"), readChunkSize+5)
	buf := appendRecord(nil, 1, []byte("a"))
	buf = appendRecord(buf, 2, large)
	buf = appendRecord(buf, 3, []byte("b"))

	// stop falls inside record 2: the record is still read whole, in one fetch.
	stop := int64(recordHeaderSize + 1 + recordHeaderSize + 100)
	src := &fetchLog{src: bytes.NewReader(buf)}
	reader := recordReader{src: src, end: int64(len(buf)), stop: stop}

	for index, want := range [][]byte{[]byte("a"), large, []byte("b")} {
		data, err := reader.next(uint64(index + 1))
		require.NoError(t, err)
		require.Equal(t, want, data)
	}

	require.Equal(t, []int{int(stop), recordHeaderSize + len(large), recordHeaderSize, recordHeaderSize + 1}, src.fetches)
}

func TestRecordReader_StopDoesNotHideShortRecord(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, 1, []byte("payload"))

	// A stop before the record's end neither shortens the fetch nor fakes a
	// short record; end alone decides that.
	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf)), stop: recordHeaderSize}
	data, err := reader.next(1)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)

	reader = recordReader{src: bytes.NewReader(buf), end: int64(len(buf)) - 1, stop: recordHeaderSize}
	_, err = reader.next(1)
	require.ErrorIs(t, err, errShortRecord)
}

func TestRecordReader_StopsAtFailingRecord(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, 1, []byte("good"))
	failAt := int64(len(buf))
	buf = appendRecord(buf, 2, []byte("bad"))
	buf[len(buf)-1] ^= 0xff

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}

	_, err := reader.next(1)
	require.NoError(t, err)

	_, err = reader.next(2)
	require.ErrorIs(t, err, errBadData)
	require.Equal(t, failAt, reader.offset)
}

func TestRecordReader_SkipsRecordWithBadData(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, 1, []byte("bad"))
	buf[len(buf)-1] ^= 0xff
	buf = appendRecord(buf, 2, []byte("next"))

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}

	_, err := reader.next(1)
	require.ErrorIs(t, err, errBadData)
	require.NoError(t, reader.skip())

	data, err := reader.next(2)
	require.NoError(t, err)
	require.Equal(t, []byte("next"), data)
}

func TestRecordReader_SkipNeedsIntactHeader(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, 1, []byte("bad"))
	buf[0] ^= 0xff

	reader := recordReader{src: bytes.NewReader(buf), end: int64(len(buf))}
	require.ErrorIs(t, reader.skip(), errBadHeader)
	require.Zero(t, reader.offset)
}

func TestRecordReader_FileShorterThanEnd(t *testing.T) {
	t.Parallel()

	buf := appendRecord(nil, 1, []byte("payload"))
	reader := recordReader{src: bytes.NewReader(buf[:len(buf)-2]), end: int64(len(buf))}

	_, err := reader.next(1)
	require.ErrorIs(t, err, errShortRecord)
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

	for _, name := range []string{"00000000000000000001.wal.tmp", "00000000000000000001.idx.tmp", "checkpoint.tmp"} {
		require.True(t, isTempName(name), name)
	}

	for _, name := range []string{
		"notes.tmp", "1.wal.tmp", "1.idx.tmp", "checkpoint", "00000000000000000001.wal", "00000000000000000001.idx", "LOCK.tmp",
	} {
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
