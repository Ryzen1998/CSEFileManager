package utils

import (
	"github.com/klauspost/compress/zip"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"path/filepath"
)

func CreateZipArchive(zipFileName, sourceFile string, logger zerolog.Logger) error {
	// Create a new zip file
	zipFile, err := os.Create(zipFileName)
	if err != nil {
		return err
	}
	defer func(zipFile *os.File) {
		err := zipFile.Close()
		if err != nil {
			logger.Err(err).Msg("error closing the zip file")
		}
	}(zipFile)

	// Create a zip writer
	zipWriter := zip.NewWriter(zipFile)
	defer func(zipWriter *zip.Writer) {
		err := zipWriter.Close()
		if err != nil {
			log.Err(err).Msg("error closing the zip writer")
		}
	}(zipWriter)

	// Add the log file to the zip archive
	return AddFileToZip(zipWriter, sourceFile, "", logger)
}

func AddFileToZip(zipWriter *zip.Writer, filePath, baseDir string, logger zerolog.Logger) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.Err(err).Msg("error closing the file")
		}
	}(file)

	// Create a header for the file in the zip archive
	zipFile, err := zipWriter.Create(filepath.Join(baseDir, filepath.Base(filePath)))
	if err != nil {
		logger.Err(err).Msg("error creating the zip file")
		return err
	}

	// Copy the file content to the zip archive
	_, err = io.Copy(zipFile, file)
	if err != nil {
		logger.Err(err).Msg("error creating the zip file")
	}
	return err
}
