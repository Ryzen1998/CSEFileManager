package utils

import (
	"CSEFileManager/models"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func WalkDirectoryAndProcessFiles(jobs []models.ArchiveJob) {
	var filePatterns []string
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, viper.GetInt("ARCHIVE_JOB_MAX_ROUTINES")) // buffered channel to limit concurrency

	for _, job := range jobs {
		log.Info().Msgf("starting job %d", job.JobId)
		filePatterns = strings.Split(job.FilePattern, job.FilePatternSeparator)

		for _, filePattern := range filePatterns {
			log.Info().Msgf("searching files with pattern %s", filePattern)

			files, err := filepath.Glob(filepath.Join(job.ArchiveFromPath, filePattern))
			if err != nil {
				log.Error().Err(err).Msgf("error searching files with pattern %s", filePattern)
				continue
			}

			if len(files) == 0 {
				log.Info().Msgf("no files found with pattern %s", filePattern)
				continue
			} else {
				log.Info().Msgf("found %d files with pattern %s", len(files), filePattern)
			}

			// start file processing
			wg.Add(1)
			semaphore <- struct{}{} // acquire a slot
			routineName := fmt.Sprintf("ROUTINE_%d", job.JobId)
			go func(files []string, routine string, job models.ArchiveJob) {
				defer wg.Done()
				defer func() { <-semaphore }() // release slot

				ProcessFiles(files, routine, job)
			}(files, routineName, job)
		}
	}
	wg.Wait() // wait for all goroutines to finish
}

func ProcessFiles(files []string, routineName string, job models.ArchiveJob) {
	logger := log.With().Str("routine", routineName).Logger()
	for _, file := range files {
		logger.Info().Msgf("processing file %s", file)
		fileInfo, err := os.Stat(file)
		if err != nil {
			logger.Err(err).Msg("unable to get file info, skipping.....")
			continue
		}

		if fileInfo.IsDir() {
			logger.Info().Msgf("file %s is directory..skipping", fileInfo.Name())
			continue
		}

		// do not process if the last mod time is lesser than what is given in property
		if job.ArchiveIfOlderThan != 0 {
			hoursDiff := time.Since(fileInfo.ModTime()).Hours()
			if hoursDiff < float64(job.ArchiveIfOlderThan) {
				log.Warn().Msgf("%s last mod time doesn't meet the criteria, last mod time %s skipping...", file, fileInfo.ModTime())
				continue
			}
		}

		lastModDate := fileInfo.ModTime().Format("2006-01-02")
		logger.Info().Msgf("creating backup folder with date %s", lastModDate)
		backupPath, err := CreateBackupFolder(job.ArchiveToPath, lastModDate, logger)
		if err != nil {
			logger.Error().Err(err).Msgf("unable to create backup folder with date %s for file %s skipping...", lastModDate, file)
			continue
		}

		// create zip file
		zipFileName := filepath.Join(backupPath, filepath.Base(file)+".zip")
		err = CreateZipArchive(zipFileName, file, logger)
		if err != nil {
			logger.Err(err).Msgf("error creating archive %s", zipFileName)
			continue
		}

		if job.DeleteOriginalFile {
			logger.Info().Msgf("deleting original file %s", filepath.Base(file))
			err = os.Remove(file)
			if err != nil {
				logger.Err(err).Msgf("unable to delete file %s after archive", file)
				continue
			}
		}

		logger.Info().Msgf("Log file %s archived to %s", file, zipFileName)
	}
}

// Utility function to check if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// Utility function to get file size
func getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
