package jobs

import (
	"CSEFileManager/models"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/spf13/viper"
)

var AppFlags models.Args

type CSVRegistry struct {
	filePath    string
	records     map[string]bool            // key: filename_jobname for quick lookup
	dateRecords map[string]map[string]bool // key: date -> filename -> true
}

func NewCSVRegistry(filePath string) *CSVRegistry {
	registry := &CSVRegistry{
		filePath:    filePath,
		records:     make(map[string]bool),
		dateRecords: make(map[string]map[string]bool),
	}
	registry.load()
	return registry
}

func RunFupmJobs(appFlags models.Args) {
	AppFlags = appFlags
	log.Info().Msg("Starting fupm uploader..")
	jobCount := viper.GetInt("FUPM_JOB_COUNT")
	log.Info().Msgf("fupm job count: %d", jobCount)

	jobList := make([]models.FupmJob, jobCount)

	for i := 0; i < jobCount; i++ {
		idx := i + 1
		log.Info().Msgf("parsing job details for job %d", idx)
		log.Info().Msgf("FUPM_FILE_PATTERN%d=%s", idx, viper.GetString("FUPM_FILE_PATTERN"+strconv.Itoa(idx)))
		log.Info().Msgf("FUPM_FILE_TRANSFER_TYPE%d=%s", idx, viper.GetString("FUPM_FILE_TRANSFER_TYPE"+strconv.Itoa(idx)))
		log.Info().Msgf("FUPM_FILE_FROM_PATH%d=%s", idx, viper.GetString("FUPM_FILE_FROM_PATH"+strconv.Itoa(idx)))
		log.Info().Msgf("FUPM_FILE_TO_PATH%d=%s", idx, viper.GetString("FUPM_FILE_TO_PATH"+strconv.Itoa(idx)))
		log.Info().Msgf("FUPM_FILE_UPLOAD_SQL_SCRIPT%d=%s", idx, viper.GetString("FUPM_FILE_UPLOAD_SQL_SCRIPT"+strconv.Itoa(idx)))
		log.Info().Msgf("FUPM_PROCESS_ONCE%d=%s", idx, viper.GetString("FUPM_PROCESS_ONCE"+strconv.Itoa(idx)))

		jobList[i] = models.FupmJob{
			JobId:                idx,
			FilePattern:          viper.GetString("FUPM_FILE_PATTERN" + strconv.Itoa(idx)),
			FileTransferType:     viper.GetString("FUPM_FILE_TRANSFER_TYPE" + strconv.Itoa(idx)),
			FileTransferFromPath: viper.GetString("FUPM_FILE_FROM_PATH" + strconv.Itoa(idx)),
			FileTransferToPath:   viper.GetString("FUPM_FILE_TO_PATH" + strconv.Itoa(idx)),
			FileUploadSqlScript:  viper.GetString("FUPM_FILE_UPLOAD_SQL_SCRIPT" + strconv.Itoa(idx)),
			ProcessOnce:          viper.GetBool("FUPM_PROCESS_ONCE" + strconv.Itoa(idx)),
		}
	}
	WalkDirAndPlayFile(jobList)
}

func WalkDirAndPlayFile(jobList []models.FupmJob) {
	log.Info().Msg("Starting file processing...")

	csvFilePath := viper.GetString("CSV_REGISTRY_PATH")
	if csvFilePath == "" {
		csvFilePath = "./processed_files.csv" // Default path
	}
	registry := NewCSVRegistry(csvFilePath)
	log.Info().Msgf("Using CSV registry: %s", csvFilePath)

	for _, job := range jobList {
		log.Info().Msgf("Processing job %d", job.JobId)
		processJobFiles(job, registry)
	}
}

func processJobFiles(job models.FupmJob, registry *CSVRegistry) {
	log.Info().Msgf("Processing files for job %d from %s", job.JobId, job.FileTransferFromPath)
	jobName := fmt.Sprintf("Job_%d_%s", job.JobId, job.FileTransferType)

	var date string
	var actualPattern string

	if AppFlags.Arg1 == "" {
		// Get today's date
		now := time.Now()
		log.Info().Msgf("No arg1 passed, using today's date")

		// Check which date format is used in the pattern
		if strings.Contains(job.FilePattern, "YYYYMMDD") {
			date = now.Format("20060102") // YYYYMMDD format
			actualPattern = strings.ReplaceAll(job.FilePattern, "YYYYMMDD", date)
			log.Info().Msgf("Using YYYYMMDD format, date: %s", date)
		} else if strings.Contains(job.FilePattern, "YYMMDD") {
			date = now.Format("060102") // YYMMDD format
			actualPattern = strings.ReplaceAll(job.FilePattern, "YYMMDD", date)
			log.Info().Msgf("Using YYMMDD format, date: %s", date)
		} else {
			// No date pattern found, use pattern as-is
			actualPattern = job.FilePattern
			date = now.Format("20060102") // Default for registry tracking
			log.Info().Msg("No date pattern found in file pattern, using as-is")
		}
	} else {
		// Use provided date from Arg1
		providedDate := AppFlags.Arg1
		log.Info().Msgf("Using date from arg1: %s", providedDate)

		// Determine format and convert if needed
		if strings.Contains(job.FilePattern, "YYYYMMDD") {
			if len(providedDate) == 6 {
				// Convert YYMMDD to YYYYMMDD
				year := providedDate[:2]
				if year > "50" { // Assume 50-99 is 1950-1999, 00-50 is 2000-2050
					date = "19" + providedDate
				} else {
					date = "20" + providedDate
				}
				log.Info().Msgf("Converted YYMMDD %s to YYYYMMDD %s", providedDate, date)
			} else if len(providedDate) == 8 {
				date = providedDate
			} else {
				log.Error().Msgf("Invalid date format in arg1: %s (expected YYMMDD or YYYYMMDD)", providedDate)
				return
			}
			actualPattern = strings.ReplaceAll(job.FilePattern, "YYYYMMDD", date)
		} else if strings.Contains(job.FilePattern, "YYMMDD") {
			if len(providedDate) == 8 {
				// Convert YYYYMMDD to YYMMDD
				date = providedDate[2:] // Take last 6 characters
				log.Info().Msgf("Converted YYYYMMDD %s to YYMMDD %s", providedDate, date)
			} else if len(providedDate) == 6 {
				date = providedDate
			} else {
				log.Error().Msgf("Invalid date format in arg1: %s (expected YYMMDD or YYYYMMDD)", providedDate)
				return
			}
			actualPattern = strings.ReplaceAll(job.FilePattern, "YYMMDD", date)
		} else {
			// No date pattern found
			actualPattern = job.FilePattern
			date = providedDate
			log.Info().Msg("No date pattern found in file pattern, using provided date for registry tracking")
		}
	}

	log.Info().Msgf("Final pattern after date replacement: %s", actualPattern)

	// Create full path pattern for glob
	fullPattern := filepath.Join(job.FileTransferFromPath, actualPattern)

	// Find all files matching the pattern
	matchingFiles, err := filepath.Glob(fullPattern)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding files with pattern %s", fullPattern)
		return
	}

	if len(matchingFiles) == 0 {
		log.Warn().Msgf("No files found matching pattern: %s", actualPattern)
		return
	}

	log.Info().Msgf("Found %d files matching pattern", len(matchingFiles))

	// Process each matching file
	for _, sourceFile := range matchingFiles {
		fileName := filepath.Base(sourceFile)
		log.Info().Msgf("Processing file: %s", fileName)

		// For registry checking, always use YYYYMMDD format for consistency
		registryDate := date
		if len(date) == 6 {
			// Convert YYMMDD to YYYYMMDD for registry
			year := date[:2]
			if year > "50" {
				registryDate = "19" + date
			} else {
				registryDate = "20" + date
			}
		}

		// Check based on ProcessOnce setting
		if job.ProcessOnce {
			// Check if file already processed on this date
			log.Debug().Msgf("ProcessOnce=true, checking if file %s was processed on date %s", fileName, registryDate)
			if registry.IsProcessedOnDate(fileName, registryDate) {
				log.Info().Msgf("File %s already processed on date %s (ProcessOnce=true), skipping", fileName, registryDate)
				continue
			}
			log.Debug().Msgf("File %s not found in date registry for %s, proceeding with processing", fileName, registryDate)
		} else {
			// Check if file already processed for this specific job
			log.Debug().Msgf("ProcessOnce=false, checking if file %s was processed by job %s", fileName, jobName)
			if registry.IsProcessed(fileName, jobName) {
				log.Info().Msgf("File %s already processed for %s, skipping", fileName, jobName)
				continue
			}
			log.Debug().Msgf("File %s not found in job registry for %s, proceeding with processing", fileName, jobName)
		}

		destinationFile := filepath.Join(job.FileTransferToPath, fileName)

		// Perform the file operation based on transfer type
		var operationErr error
		switch strings.ToUpper(job.FileTransferType) {
		case "COPY":
			operationErr = copyFile(sourceFile, destinationFile)
			if operationErr == nil {
				log.Info().Msgf("Successfully copied: %s -> %s", sourceFile, destinationFile)
			}
		case "MOVE":
			operationErr = moveFile(sourceFile, destinationFile)
			if operationErr == nil {
				log.Info().Msgf("Successfully moved: %s -> %s", sourceFile, destinationFile)
			}
		default:
			log.Error().Msgf("Unknown transfer type: %s for job %d", job.FileTransferType, job.JobId)
			continue
		}

		// If operation was successful, add to CSV registry
		if operationErr == nil {
			if err := registry.AddFile(jobName, fileName, destinationFile); err != nil {
				log.Error().Err(err).Msgf("Failed to add file %s to CSV registry", fileName)
			} else {
				log.Info().Msgf("Added file %s to CSV registry", fileName)

				if job.FileUploadSqlScript != "" {
					log.Info().Msg("SQL Script found... starting insert job...")
					InsertFupm(job, fileName)
				}
			}
		} else {
			log.Error().Err(operationErr).Msgf("Failed to %s file %s", strings.ToLower(job.FileTransferType), fileName)
		}
	}
}

func InsertFupm(job models.FupmJob, fileName string) {
	var connectionString string
	log.Info().Msgf("Initiating oracle SQL connection for job %d", job.JobId)

	if viper.GetString("FUPM_ORCL_SRV_NAME") != "" {
		log.Info().Msg("Oracle service name found... building connection with service name")
		connectionString = go_ora.BuildUrl(
			viper.GetString("FUPM_ORCL_HOST"),
			viper.GetInt("FUPM_ORCL_PORT"),
			viper.GetString("FUPM_ORCL_SRV_NAME"),
			viper.GetString("FUPM_ORCL_USR_NAME"),
			viper.GetString("FUPM_ORCL_PASS"),
			nil,
		)
	} else {
		log.Info().Msg("Oracle service name not found... building connection with SID")
		urlOptions := map[string]string{
			"SID": viper.GetString("FUPM_ORCL_SID"),
		}
		connectionString = go_ora.BuildUrl(
			viper.GetString("FUPM_ORCL_HOST"),
			viper.GetInt("FUPM_ORCL_PORT"),
			"",
			viper.GetString("FUPM_ORCL_USR_NAME"),
			viper.GetString("FUPM_ORCL_PASS"),
			urlOptions,
		)
	}

	conn, err := sql.Open("oracle", connectionString)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to connect to oracle service")
		return
	}
	err = conn.Ping()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to ping oracle service")
		return
	} else {
		log.Info().Msg("Successfully pinged oracle service")
	}
	defer func() {
		conn.Close()
	}()
	log.Info().Msgf("inserting into fupm with %s", job.FileUploadSqlScript)
	sqlQueryReplacements := map[string]string{
		"FILENAME":    fileName,
		"NEWFILENAME": fileName,
		"LOCATION":    job.FileTransferToPath,
		"FILESIZE":    "0",
	}
	query := job.FileUploadSqlScript
	for key, value := range sqlQueryReplacements {
		query = strings.ReplaceAll(query, key, value)
	}
}

func copyFile(src, dst string) error {
	log.Debug().Msgf("Copying file from %s to %s", src, dst)

	// Create destination directory if it doesn't exist
	destDir := filepath.Dir(dst)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destDir, err)
	}

	// Open source file
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer sourceFile.Close()

	// Create destination file
	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer destFile.Close()

	// Copy file contents
	bytesWritten, err := io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	log.Debug().Msgf("Copied %d bytes", bytesWritten)

	// Sync to ensure data is written to disk
	if err := destFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}

	return nil
}

func moveFile(src, dst string) error {
	log.Debug().Msgf("Moving file from %s to %s", src, dst)

	// Create destination directory if it doesn't exist
	destDir := filepath.Dir(dst)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destDir, err)
	}

	// Try to rename first (fastest for same filesystem)
	if err := os.Rename(src, dst); err != nil {
		// If rename fails (e.g., different filesystems), copy then delete
		log.Debug().Msgf("Rename failed, falling back to copy+delete: %v", err)

		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("failed to copy file during move operation: %w", err)
		}

		if err := os.Remove(src); err != nil {
			return fmt.Errorf("failed to remove source file after copy: %w", err)
		}

		log.Debug().Msg("Move completed via copy+delete")
	} else {
		log.Debug().Msg("Move completed via rename")
	}

	return nil
}

func (cr *CSVRegistry) load() {
	file, err := os.Open(cr.filePath)
	if err != nil {
		log.Info().Msgf("CSV registry file %s doesn't exist yet, will be created", cr.filePath)
		return // File doesn't exist yet
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Error().Err(err).Msg("Failed to read CSV registry")
		return
	}

	log.Info().Msgf("Reading CSV file with %d total lines", len(records))

	for i, record := range records {
		if i == 0 { // Skip header
			if len(record) > 0 {
				log.Debug().Msgf("CSV Header: %v", record)
			}
			continue
		}
		if len(record) >= 4 {
			dateTime := record[0]
			jobName := record[1]
			filename := record[2]

			log.Debug().Msgf("Loading record: DateTime=%s, JobName=%s, FileName=%s", dateTime, jobName, filename)

			// Create key from filename and job name for quick lookup
			key := fmt.Sprintf("%s_%s", filename, jobName) // filename_jobname
			cr.records[key] = true

			// Extract date from datetime and store in dateRecords
			if len(dateTime) >= 10 {
				date := dateTime[:10] // Extract YYYY-MM-DD part

				if cr.dateRecords[date] == nil {
					cr.dateRecords[date] = make(map[string]bool)
				}
				cr.dateRecords[date][filename] = true
				log.Debug().Msgf("Added to dateRecords: date=%s, filename=%s", date, filename)
			}
		} else {
			log.Warn().Msgf("Skipping invalid CSV record at line %d: %v", i+1, record)
		}
	}
	log.Info().Msgf("Loaded %d processed file records from CSV", len(cr.records))
	log.Info().Msgf("Loaded date records for %d dates", len(cr.dateRecords))
}

func (cr *CSVRegistry) IsProcessed(filename, jobName string) bool {
	key := fmt.Sprintf("%s_%s", filename, jobName)
	isProcessed := cr.records[key]
	log.Debug().Msgf("Checking IsProcessed: key=%s, result=%t", key, isProcessed)
	return isProcessed
}

func (cr *CSVRegistry) IsProcessedOnDate(filename, date string) bool {
	// Convert YYYYMMDD to YYYY-MM-DD format for comparison
	originalDate := date
	if len(date) == 8 {
		date = fmt.Sprintf("%s-%s-%s", date[:4], date[4:6], date[6:8])
	}

	log.Debug().Msgf("Checking IsProcessedOnDate: filename=%s, originalDate=%s, convertedDate=%s", filename, originalDate, date)

	if dateMap, exists := cr.dateRecords[date]; exists {
		isProcessed := dateMap[filename]
		log.Debug().Msgf("Date map exists for %s, checking filename %s: %t", date, filename, isProcessed)
		return isProcessed
	}

	log.Debug().Msgf("No date map found for %s", date)
	return false
}

func (cr *CSVRegistry) AddFile(jobName, filename, newFilePath string) error {
	now := time.Now()
	dateStr := now.Format("2006-01-02") // YYYY-MM-DD format

	log.Info().Msgf("Adding file to registry: jobName=%s, filename=%s, date=%s", jobName, filename, dateStr)

	// Add to memory map for quick lookup
	key := fmt.Sprintf("%s_%s", filename, jobName)
	cr.records[key] = true
	log.Debug().Msgf("Added to records map: key=%s", key)

	// Add to dateRecords for date-based lookup
	if cr.dateRecords[dateStr] == nil {
		cr.dateRecords[dateStr] = make(map[string]bool)
	}
	cr.dateRecords[dateStr][filename] = true
	log.Debug().Msgf("Added to dateRecords: date=%s, filename=%s", dateStr, filename)

	// Check if file exists and needs header
	fileExists := true
	if _, err := os.Stat(cr.filePath); os.IsNotExist(err) {
		fileExists = false
		log.Info().Msgf("CSV file %s does not exist, will create with header", cr.filePath)
	}

	// Open file for appending
	file, err := os.OpenFile(cr.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	// Write header if file is new
	if !fileExists {
		header := []string{"DateTime", "JobName", "FileName", "NewFilePath"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
		log.Info().Msgf("Created new CSV registry file with header: %s", cr.filePath)
	}

	// Write the record
	record := []string{
		now.Format("2006-01-02 15:04:05"),
		jobName,
		filename,
		newFilePath,
	}

	log.Debug().Msgf("Writing CSV record: %v", record)
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}

	// IMPORTANT: Flush immediately to ensure data is written
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	// Force sync to disk to ensure data persistence
	if err := file.Sync(); err != nil {
		log.Warn().Err(err).Msg("Failed to sync CSV file to disk")
	}

	log.Info().Msgf("Successfully added file to CSV registry: %s", filename)
	return nil
}
