package main

import (
	"CSEFileManager/jobs"
	"CSEFileManager/models"
	"flag"
	"github.com/natefinch/lumberjack"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/viper"
	"os"
)

// Flags declared globally, so they can be used in both init() and main()
var (
	configName = flag.String("config-name", "settings", "Name of the config file (without extension)")
	configPath = flag.String("config-path", ".", "Path to the config file directory")
	jobType    = flag.String("job-type", "ARCHIVE", "Type of job to execute")
	Arg1       = flag.String("arg1", "", "Argument 1 (optional)")
)

func main() {
	appFlags := models.Args{
		ConfigName: *configName,
		ConfigPath: *configPath,
		JobType:    *jobType,
		Arg1:       *Arg1,
	}

	if *jobType == "" || *jobType == "ARCHIVE" {
		log.Info().Msg("starting job..")
		jobs.RunArchiver()
	} else if *jobType == "FUPM" {
		jobs.RunFupmJobs(appFlags)
	}
	log.Info().Msg("Job executed successfully")
}

func init() {
	log.Info().Msg("initiating file manager...")
	log.Info().Msg("reading config file...")

	// Parse flags
	flag.Parse()

	log.Info().Msgf("using config file name: %s", *configName)
	log.Info().Msgf("using config file path: %s", *configPath)

	// Positional args after flags
	positionalArgs := flag.Args()
	if len(positionalArgs) > 0 {
		log.Info().Msgf("positional args: %v", positionalArgs)
		// You can process these if needed
	}

	viper.SetConfigName(*configName) //get from param
	viper.AddConfigPath(*configPath) //get from param
	err := viper.ReadInConfig()
	if err != nil {
		log.Error().Err(err).Msg("unable to read config file, program will exit now")
		os.Exit(1)
	}

	//log
	log.Info().Msg("initializing logger...")
	logFile := &lumberjack.Logger{
		Filename:   viper.GetString("LOG_PATH"),
		MaxSize:    viper.GetInt("LOG_MAX_SIZE"),   // MB
		MaxBackups: viper.GetInt("LOG_MAX_BACKUP"), // Number of backups to keep
		MaxAge:     viper.GetInt("LOG_MAX_AGE"),    // Days
		Compress:   viper.GetBool("LOG_COMPRESS"),
	}
	zerolog.TimeFieldFormat = viper.GetString("LOG_DATETIME_PATTERN")
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	multi := zerolog.MultiLevelWriter(os.Stdout, logFile)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()
}
