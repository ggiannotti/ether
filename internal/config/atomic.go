package config

import (
	"os"
	"path/filepath"
	"runtime"
)

// writeFileAtomic replaces path with data so that a crash or power loss
// leaves either the old or the new file, never a truncated one: it writes a
// uniquely named temp file in the same directory (concurrent writers or a
// stale temp from a crash cannot collide), fsyncs it, renames it over path
// and fsyncs the directory so the rename itself is durable. Directory fsync
// is skipped on Windows, where it is unsupported and rename is already
// journalled by NTFS.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil && runtime.GOOS != "windows" {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	if runtime.GOOS != "windows" {
		if d, err := os.Open(dir); err == nil {
			if err := d.Sync(); err != nil {
				_ = d.Close()
				return err
			}
			// Close of a read-only directory after a successful Sync can
			// report a spurious failure even though the rename is already
			// durable; do not turn that into a failed write.
			_ = d.Close()
		}
	}
	return nil
}
