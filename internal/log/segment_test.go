package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/Franklynoble/proglog/api/v1"
)

/*
We test that we can append a record to a segment, read back the same record,
and eventually hit the configured max size for both the store and index.
Calling newSegment() twice with the same base offset and dir also checks that
the function loads a segment’s state from the persisted index and log files.
Now that we know that our segment works, we’re ready to create the log
*/
//
// func TestSegment(t *testing.T) {
// dir, _ := ioutil.TempDir("", "segment-test")
//
// defer os.RemoveAll(dir)
//
// want := &api.Record{
// Value: []byte("hello world")}
// c := Config{}
// c.Segment.MaxStoreBytes = 1024
// c.Segment.MaxIndexBytes = entWidth * 3
//
// s, err := newSegment(dir, 16, c)
//
// require.NoError(t, err)
// require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
// require.False(t, s.IsMaxed())
//
// for i := uint64(0); i < 3; i++ {
// off, err := s.Append(want)
// require.NoError(t, err)
// require.Equal(t, 16+i, off)
// got, err := s.Read(off)
// require.NoError(t, err)
// require.Equal(t, want.Value, got.Value)
// }
//
// _, err = s.Append(want)
// require.Equal(t, io.EOF, err)
//
//maxed index
// require.True(t, s.IsMaxed())
//
// c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
// c.Segment.MaxIndexBytes = 1024
//
// s, err = newSegment(dir, 16, c)
// require.NoError(t, err)
//maxed store
// require.True(t, s.IsMaxed())
//
// err = s.Remove()
// require.NoError(t, err)
// s, err = newSegment(dir, 16, c)
// require.NoError(t, err)
// require.False(t, s.IsMaxed())
// }
//

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// maxed store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

}
