package utils

import (
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func CreateBackupFolder(rootFolder string, lastModDate string, logger zerolog.Logger) (string, error) {
	var year, month, day string
	dateArray := strings.Split(lastModDate, "-")

	if len(dateArray) == 3 {
		year = dateArray[0]
		month = dateArray[1]
		day = dateArray[2]
	} else {
		currentTime := time.Now()
		year = currentTime.Format("2006")
		month = currentTime.Format("01")
		day = currentTime.Format("02")
	}

	backupFolder := filepath.Join(rootFolder, year, month, day)
	logger.Info().Msgf("attempting to create backup folder %s", backupFolder)
	err := os.MkdirAll(backupFolder, os.ModePerm)
	if err != nil {
		logger.Err(err).Msgf("unable to create backup folder %s", backupFolder)
		return "", err
	}

	return backupFolder, nil
}
