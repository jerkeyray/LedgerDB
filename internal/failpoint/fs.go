package failpoint

import (
	"io/fs"
	"sync"

	"github.com/jerkeyray/ledgerdb"
)

type Operation string

const (
	OpMkdirAll Operation = "mkdir_all"
	OpOpen     Operation = "open"
	OpReadFile Operation = "read_file"
	OpReadDir  Operation = "read_dir"
	OpStat     Operation = "stat"
	OpLock     Operation = "lock"
	OpWrite    Operation = "write"
	OpSync     Operation = "sync"
	OpTruncate Operation = "truncate"
	OpClose    Operation = "close"
	OpRename   Operation = "rename"
	OpRemove   Operation = "remove"
)

// Failure triggers on the At-th occurrence of Op. Partial controls how many
// bytes an intercepted Write passes to the underlying file before returning
// Err. A negative Partial means that no bytes are written.
type Failure struct {
	Op      Operation
	At      int
	After   bool
	Partial int
	Err     error
	Crash   func()

	used bool
}

type FS struct {
	Base ledgerdb.FileSystem

	mu       sync.Mutex
	counts   map[Operation]int
	failures []*Failure
}

func New(base ledgerdb.FileSystem) *FS {
	if base == nil {
		base = ledgerdb.NewOSFileSystem()
	}
	return &FS{Base: base, counts: make(map[Operation]int)}
}

func (f *FS) Add(failure Failure) {
	if failure.At <= 0 {
		failure.At = 1
	}
	f.mu.Lock()
	f.failures = append(f.failures, &failure)
	f.mu.Unlock()
}

func (f *FS) Count(operation Operation) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[operation]
}

func (f *FS) trigger(operation Operation, after bool, occurrence int) (int, *Failure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !after {
		f.counts[operation]++
		occurrence = f.counts[operation]
	}
	for _, failure := range f.failures {
		if !failure.used && failure.Op == operation && failure.At == occurrence && failure.After == after {
			failure.used = true
			copyOf := *failure
			return occurrence, &copyOf
		}
	}
	return occurrence, nil
}

func fire(failure *Failure) error {
	if failure == nil {
		return nil
	}
	if failure.Crash != nil {
		failure.Crash()
	}
	return failure.Err
}

func (f *FS) MkdirAll(path string, mode fs.FileMode) error {
	count, failure := f.trigger(OpMkdirAll, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.Base.MkdirAll(path, mode)
	if _, failure = f.trigger(OpMkdirAll, true, count); failure != nil {
		return fire(failure)
	}
	return err
}

func (f *FS) OpenFile(path string, flag int, mode fs.FileMode) (ledgerdb.File, error) {
	count, failure := f.trigger(OpOpen, false, 0)
	if failure != nil {
		return nil, fire(failure)
	}
	file, err := f.Base.OpenFile(path, flag, mode)
	if err != nil {
		return nil, err
	}
	if _, failure = f.trigger(OpOpen, true, count); failure != nil {
		_ = file.Close()
		return nil, fire(failure)
	}
	return &wrappedFile{File: file, owner: f}, nil
}

func (f *FS) ReadFile(path string) ([]byte, error) {
	count, failure := f.trigger(OpReadFile, false, 0)
	if failure != nil {
		return nil, fire(failure)
	}
	data, err := f.Base.ReadFile(path)
	if _, failure = f.trigger(OpReadFile, true, count); failure != nil {
		return nil, fire(failure)
	}
	return data, err
}

func (f *FS) ReadDir(path string) ([]fs.DirEntry, error) {
	count, failure := f.trigger(OpReadDir, false, 0)
	if failure != nil {
		return nil, fire(failure)
	}
	entries, err := f.Base.ReadDir(path)
	if _, failure = f.trigger(OpReadDir, true, count); failure != nil {
		return nil, fire(failure)
	}
	return entries, err
}

func (f *FS) Stat(path string) (fs.FileInfo, error) {
	count, failure := f.trigger(OpStat, false, 0)
	if failure != nil {
		return nil, fire(failure)
	}
	info, err := f.Base.Stat(path)
	if _, failure = f.trigger(OpStat, true, count); failure != nil {
		return nil, fire(failure)
	}
	return info, err
}

func (f *FS) Lock(path string) (ledgerdb.Lock, error) {
	count, failure := f.trigger(OpLock, false, 0)
	if failure != nil {
		return nil, fire(failure)
	}
	lockingBase, ok := f.Base.(ledgerdb.LockingFileSystem)
	if !ok {
		return nil, ledgerdb.ErrLockUnsupported
	}
	lock, err := lockingBase.Lock(path)
	if _, failure = f.trigger(OpLock, true, count); failure != nil {
		if lock != nil {
			_ = lock.Close()
		}
		return nil, fire(failure)
	}
	return lock, err
}

func (f *FS) Rename(oldPath, newPath string) error {
	count, failure := f.trigger(OpRename, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.Base.Rename(oldPath, newPath)
	if _, failure = f.trigger(OpRename, true, count); failure != nil {
		return fire(failure)
	}
	return err
}

func (f *FS) Remove(path string) error {
	count, failure := f.trigger(OpRemove, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.Base.Remove(path)
	if _, failure = f.trigger(OpRemove, true, count); failure != nil {
		return fire(failure)
	}
	return err
}

type wrappedFile struct {
	ledgerdb.File
	owner *FS
}

func (f *wrappedFile) Write(data []byte) (int, error) {
	count, failure := f.owner.trigger(OpWrite, false, 0)
	if failure != nil {
		if failure.Crash != nil {
			failure.Crash()
		}
		if failure.Partial >= 0 {
			limit := failure.Partial
			if limit > len(data) {
				limit = len(data)
			}
			n, err := f.File.Write(data[:limit])
			if err != nil {
				return n, err
			}
			return n, failure.Err
		}
		return 0, failure.Err
	}
	n, err := f.File.Write(data)
	if _, failure = f.owner.trigger(OpWrite, true, count); failure != nil {
		return n, fire(failure)
	}
	return n, err
}

func (f *wrappedFile) Sync() error {
	count, failure := f.owner.trigger(OpSync, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.File.Sync()
	if _, failure = f.owner.trigger(OpSync, true, count); failure != nil {
		return fire(failure)
	}
	return err
}

func (f *wrappedFile) Truncate(size int64) error {
	count, failure := f.owner.trigger(OpTruncate, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.File.Truncate(size)
	if _, failure = f.owner.trigger(OpTruncate, true, count); failure != nil {
		return fire(failure)
	}
	return err
}

func (f *wrappedFile) Close() error {
	count, failure := f.owner.trigger(OpClose, false, 0)
	if failure != nil {
		return fire(failure)
	}
	err := f.File.Close()
	if _, failure = f.owner.trigger(OpClose, true, count); failure != nil {
		return fire(failure)
	}
	return err
}
