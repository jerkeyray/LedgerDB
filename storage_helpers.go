package ledgerdb

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func durableMkdirAll(filesystem FileSystem, path string) error {
	clean := filepath.Clean(path)
	var missing []string
	current := clean
	for {
		info, err := filesystem.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("ledgerdb: %s is not a directory", current)
			}
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return err
		}
		current = parent
	}

	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := filesystem.MkdirAll(directory, 0o700); err != nil {
			return err
		}
		if err := syncDirectoryPath(filesystem, directory); err != nil {
			return err
		}
		if err := syncDirectoryPath(filesystem, filepath.Dir(directory)); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectoryPath(filesystem FileSystem, path string) error {
	directory, err := filesystem.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func cleanupTemporaryFiles(filesystem FileSystem, dir string) error {
	entries, err := filesystem.ReadDir(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		name := entry.Name()
		if name != manifestName+".tmp" && !(strings.HasPrefix(name, "checkpoint-") && strings.HasSuffix(name, ".dat.tmp")) {
			continue
		}
		if err := filesystem.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		removed = true
	}
	if removed {
		return syncDirectoryPath(filesystem, dir)
	}
	return nil
}
