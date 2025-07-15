package models

type FupmJob struct {
	JobId                int    `json:"job_id"`
	FilePattern          string `json:"file_pattern"`
	FileTransferType     string `json:"file_transfer_type"`
	FileTransferFromPath string `json:"file_transfer_from_path"`
	FileTransferToPath   string `json:"file_transfer_to_path"`
	FileUploadSqlScript  string `json:"file_upload_sql_script"`
	ProcessOnce          bool   `json:"process_once"`
}
