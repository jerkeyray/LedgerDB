package ledgerdb

import (
	"io"
	"io/fs"
	"os"
)

// File is the subset of os.File used by LedgerDB. It is public so fault-injecting
// file systems can be supplied in tests.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	Stat() (fs.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

// FileSystem abstracts durable file operations for fault injection. Open also
// requires the supplied value to implement LockingFileSystem.
type FileSystem interface {
	MkdirAll(string, fs.FileMode) error
	OpenFile(string, int, fs.FileMode) (File, error)
	ReadFile(string) ([]byte, error)
	ReadDir(string) ([]fs.DirEntry, error)
	Stat(string) (fs.FileInfo, error)
	Rename(string, string) error
	Remove(string) error
}

// Lock is an exclusive database-directory lock held for the lifetime of a DB.
type Lock interface {
	Close() error
}

// LockingFileSystem is required by Open so recovery and WAL tail repair can
// never race another writer.
type LockingFileSystem interface {
	FileSystem
	Lock(string) (Lock, error)
}

type osFileSystem struct{}

// NewOSFileSystem returns LedgerDB's production filesystem implementation.
// It is primarily useful as the base for fault-injecting test filesystems.
func NewOSFileSystem() LockingFileSystem { return osFileSystem{} }

func (osFileSystem) MkdirAll(path string, mode fs.FileMode) error { return os.MkdirAll(path, mode) }
func (osFileSystem) OpenFile(path string, flag int, mode fs.FileMode) (File, error) {
	return os.OpenFile(path, flag, mode)
}
func (osFileSystem) ReadFile(path string) ([]byte, error)       { return os.ReadFile(path) }
func (osFileSystem) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }
func (osFileSystem) Stat(path string) (fs.FileInfo, error)      { return os.Stat(path) }
func (osFileSystem) Rename(oldPath, newPath string) error       { return os.Rename(oldPath, newPath) }
func (osFileSystem) Remove(path string) error                   { return os.Remove(path) }
