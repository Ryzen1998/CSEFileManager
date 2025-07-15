package jobs

import (
	"CSEFileManager/models"
	"CSEFileManager/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"strconv"
)

func RunArchiver() {
	log.Info().Msg("Starting archiver..")
	jobCount := viper.GetInt("ARCHIVE_JOB_COUNT")
	log.Info().Msgf("archive job count: %d", jobCount)

	jobList := make([]models.ArchiveJob, jobCount)
	for i := 0; i < jobCount; i++ {
		idx := i + 1
		log.Info().Msgf("parsing job details for job %d", idx)
		log.Info().Msgf("ARCHIVE_FROM_PATH%d=%s", idx, viper.GetString("ARCHIVE_FROM_PATH"+strconv.Itoa(idx)))
		log.Info().Msgf("ARCHIVE_TO_PATH%d=%s", idx, viper.GetString("ARCHIVE_TO_PATH"+strconv.Itoa(idx)))
		log.Info().Msgf("ARCHIVE_FILE_PATTERNS%d=%s", idx, viper.GetString("ARCHIVE_FILE_PATTERNS"+strconv.Itoa(idx)))
		log.Info().Msgf("ARCHIVE_PATTERN_SEPARATOR%d=%s", idx, viper.GetString("ARCHIVE_PATTERN_SEPARATOR"+strconv.Itoa(idx)))
		log.Info().Msgf("ARCHIVE_OLDER_THAN%d=%s", idx, viper.GetString("ARCHIVE_OLDER_THAN"+strconv.Itoa(idx)))
		log.Info().Msgf("ARCHIVE_DELETE_ORIGINAL_FILE%d=%s", idx, viper.GetString("ARCHIVE_DELETE_ORIGINAL_FILE"+strconv.Itoa(idx)))

		jobList[i] = models.ArchiveJob{
			JobId:                idx,
			ArchiveFromPath:      viper.GetString("ARCHIVE_FROM_PATH" + strconv.Itoa(idx)),
			ArchiveToPath:        viper.GetString("ARCHIVE_TO_PATH" + strconv.Itoa(idx)),
			FilePattern:          viper.GetString("ARCHIVE_FILE_PATTERNS" + strconv.Itoa(idx)),
			FilePatternSeparator: viper.GetString("ARCHIVE_PATTERN_SEPARATOR" + strconv.Itoa(idx)),
			ArchiveIfOlderThan: func() int {
				val := viper.GetInt("ARCHIVE_OLDER_THAN" + strconv.Itoa(idx))
				if val == 0 {
					val = 24
				}
				return val
			}(),
			DeleteOriginalFile: viper.GetBool("ARCHIVE_DELETE_ORIGINAL_FILE" + strconv.Itoa(idx)),
		}
	}
	utils.WalkDirectoryAndProcessFiles(jobList)
	log.Info().Msg("Archiving completed")
}
