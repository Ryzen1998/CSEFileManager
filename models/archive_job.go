package models

type ArchiveJob struct {
	JobId                int    `json:"job_id"`
	ArchiveFromPath      string `json:"archive_from_path"`
	ArchiveToPath        string `json:"archive_to_archive"`
	FilePattern          string `json:"file_pattern"`
	FilePatternSeparator string `json:"file_pattern_separator"`
	ArchiveIfOlderThan   int    `json:"archive_if_older_than"`
	DeleteOriginalFile   bool   `json:"delete_original_file"`
	Processed            bool   `json:"processed"`
}
