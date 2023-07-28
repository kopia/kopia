//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package filehandler provides the tools that handling files.
package filehandler

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// FileHandler implements a FileHandler struct.
type FileHandler struct{}

// GetRootDir browses through the provided file path and return a path, ensuring that the first item is a file and not a folder. If the first item is a folder, it will continue to open directories until the condition of the first item being a file is met.
func (handler *FileHandler) GetRootDir(source string) string {
	path := source

	for {
		dirEntries, err := os.ReadDir(path)
		if err != nil {
			log.Println(err)
			return ""
		}

		if len(dirEntries) == 0 || !dirEntries[0].IsDir() {
			break
		}

		path = filepath.Join(path, dirEntries[0].Name())
	}

	return path
}

// ModifyDataSetWithContent appends the specified content to all files in the provided folder.
func (handler *FileHandler) ModifyDataSetWithContent(destination, content string) error {
	dstDirs, err := os.ReadDir(destination)
	if err != nil {
		return err
	}

	for _, dstFile := range dstDirs {
		dstFilePath := filepath.Join(destination, dstFile.Name())

		dstFile, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}

		_, err = dstFile.WriteString(content)

		if err != nil {
			return err
		}

		dstFile.Close()
	}

	return nil
}

// CopyAllFiles implements copying all the files from a source folder to a destination folder.
func (handler *FileHandler) CopyAllFiles(source, destination string) error {
	// Create the destination folder if it doesn't exist
	err := os.MkdirAll(destination, 0o755)
	if err != nil {
		return err
	}

	srcDirs, err := os.ReadDir(source)
	if err != nil {
		return err
	}

	for _, file := range srcDirs {
		sourcePath := filepath.Join(source, file.Name())
		destinationPath := filepath.Join(destination, file.Name())

		// Open the source file
		sourceFile, err := os.Open(sourcePath)
		if err != nil {
			return err
		}

		destinationFile, err := os.Create(destinationPath)
		if err != nil {
			return err
		}

		_, err = io.Copy(destinationFile, sourceFile)
		if err != nil {
			return err
		}

		sourceFile.Close()

		destinationFile.Close()
	}

	return nil
}
