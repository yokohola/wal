package wal

import (
	"bytes"
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

			buf, err := appendRecord([]byte("prefix"), data)
			require.NoError(t, err)
			require.Len(t, buf, len("prefix")+recordHeaderSize+len(data))

			got, n, err := decodeRecord(buf[len("prefix"):])
			require.NoError(t, err)
			require.Equal(t, recordHeaderSize+len(data), n)
			require.Equal(t, data, got)
		})
	}
}

func frame(t *testing.T, data []byte) []byte {
	t.Helper()

	buf, err := appendRecord(nil, data)
	require.NoError(t, err)

	return buf
}

func TestRecord_DecodeRejectsDamage(t *testing.T) {
	t.Parallel()

	valid := frame(t, []byte("payload"))

	flipped := bytes.Clone(valid)
	flipped[len(flipped)-1] ^= 0x01

	overflow := bytes.Clone(valid)
	overflow[0] = 0xff

	cases := map[string][]byte{
		"empty":         {},
		"short header":  valid[:recordHeaderSize-1],
		"short body":    valid[:len(valid)-1],
		"flipped byte":  flipped,
		"zero header":   make([]byte, 64),
		"length beyond": overflow,
	}

	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, err := decodeRecord(b)
			require.ErrorIs(t, err, errTorn)
		})
	}
}

func TestRecord_EmptyRecordDiffersFromZeroFill(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, make([]byte, recordHeaderSize), frame(t, nil))
}

func TestSegmentFileName_RoundTrip(t *testing.T) {
	t.Parallel()

	require.Equal(t, "00000000000000000001.wal", segmentFileName(1))
	require.Equal(t, "18446744073709551615.wal", segmentFileName(^uint64(0)))

	for _, name := range []string{"00000000000000000001.wal", "00000000000000000042.wal"} {
		first, ok := parseSegmentFileName(name)
		require.True(t, ok, name)
		require.Equal(t, name, segmentFileName(first))
	}

	for _, name := range []string{"1.wal", "checkpoint", "LOCK", "00000000000000000001.wal.tmp", "0000000000000000000a.wal"} {
		_, ok := parseSegmentFileName(name)
		require.False(t, ok, name)
	}
}

func TestCheckpoint_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	index, ok, err := readCheckpoint(dir)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, index)

	require.NoError(t, writeCheckpoint(dir, 42))
	require.NoError(t, writeCheckpoint(dir, 43))

	index, ok, err = readCheckpoint(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(43), index)

	require.NoError(t, os.WriteFile(filepath.Join(dir, checkpointFile), []byte("short"), 0o644))

	_, _, err = readCheckpoint(dir)
	require.ErrorIs(t, err, ErrCorrupt)
}
