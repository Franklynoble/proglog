package log

import (
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
		f.Name(), int64(c.Segment.MaxindexBytes),
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
