package models

type CSVRegistry struct {
	filePath string
	records  map[string]bool // key: filename_jobname for quick lookup
}
