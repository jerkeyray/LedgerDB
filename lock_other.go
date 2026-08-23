//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package ledgerdb

func (osFileSystem) Lock(string) (Lock, error) {
	return nil, ErrLockUnsupported
}
