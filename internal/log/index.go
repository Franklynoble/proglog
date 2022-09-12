package log

import (
	"io"
	"os"

	//"github.com/tysontate/gommap"
	"github.com/tysontate/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

/*
newIndex(*os.File) creates an index for the given file. We create the index and
save the current size of the file so we can track the amount of data in the
index file as we add index entries. We grow the file to the max index size before
memory-mapping the file and then return the created index to the caller.
*/
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	if err := os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	//Memory Mapping  the code
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil

}

func (i *index) Close() error {
	/*
		Sync flushes changes made to the region determined by
		the mmap slice back to the device. Without calling this method, there are no guarantees that changes will be flushed back before the region is unmapped. The flags parameter specifies whether flushing should be done synchronously (before the method returns) with MS_SYNC,
		or asynchronously (flushing is just scheduled) with
	*/
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Sync commits the current contents of the file to stable storage. Typically, this means flushing the
	// file system's in-memory copy of recently written data to dis
	if err := i.file.Sync(); err != nil {
		return err
	}
	//changes the size of the file, it does not change the I/O
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	/*
		Close() makes sure the memory-mapped file has synced its data to the persisted
		file and that the persisted file has flushed its contents to stable storage
	*/
	//close the file after
	return i.file.Close()
}

/*
Read(int64) takes in an offset and returns the associated record’s position in
the store. The given offset is relative to the segment’s base offset; 0 is always
the offset of the index’s first entry, 1 is the second entry, and so on. We use
relative offsets to reduce the size of the indexes by storing offsets as uint32s.
If we used absolute offsets, we’d have to store the offsets as uint64s and
require four more bytes for each entry. Four bytes doesn’t sound like much,
*/
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {

	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

/*
appends the given offset and position to the index.
First, we validate that we have space to write the entry. If there’s space, we
then encode the offset and position and write them to the memory-mapped
file. Then we increment the position where the next write will go.
*/
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
