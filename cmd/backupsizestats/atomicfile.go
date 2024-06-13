package main

import (
	"fmt"
	"os"
)

func WriteFileAtomically(path string, data []byte) error {
	tmpPath := path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("unable to create tmp file at '%s': %w", tmpPath, err)
	}
	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("unable to write to tmp file at '%s': %w", tmpPath, err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("unable to sync tmp file at '%s': %w", tmpPath, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("unable to close tmp file at '%s': %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("unable to mv tmp file '%s' to '%s': %w", tmpPath, path, err)
	}

	return nil
}
