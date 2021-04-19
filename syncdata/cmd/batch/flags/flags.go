package flags

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	goflags "github.com/jessevdk/go-flags"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/cmds"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/input_file"
	qiniu_sdk "github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gorm_logger "gorm.io/gorm/logger"
)

var (
	ErrEmptyCmd        = errors.New("empty cmd")
	ErrUnknownCmd      = errors.New("unknown command")
	ErrEmptyOptions    = errors.New("empty options")
	ErrInvalidLogLevel = errors.New("invalid log level")
)

type CmdError struct {
	err error
}

func (err CmdError) Error() string {
	return err.err.Error()
}

func (err CmdError) Unwrap() error {
	return err.err
}

type DatabaseError struct {
	err error
}

func (err DatabaseError) Error() string {
	return fmt.Sprintf("database operation error: %s", err.err)
}

func (err DatabaseError) Unwrap() error {
	return err.err
}

type InputFileParseError struct {
	err error
}

func (err InputFileParseError) Error() string {
	return fmt.Sprintf("failed to parse input file: %s", err.err)
}

func (err InputFileParseError) Unwrap() error {
	return err.err
}

type flags struct {
	ConfigFile  string `long:"config-file" default:"cfg.toml" description:"Qiniu config file, toml or json format"`
	Concurrency uint   `long:"concurrency" default:"1" description:"Concurrency of API calls"`
	DbFile      string `long:"db-file" default:"batch.db" description:"Progress database file"`
	LogFile     string `long:"log-file" default:"batch.log" description:"Log file"`
	LogLevel    string `long:"log-level" default:"warn" choice:"debug" choice:"info" choice:"warn" choice:"error" description:"Log level"`
	BatchSize   uint   `long:"batch-size" default:"100" description:"Size of operations in one batch, must be lower than or equal to 1000"`
	OnlyFailed  bool   `long:"only-failed" description:"Only show failed operations"`
	MigrateDb   bool   `long:"migrate-db" description:"Migrate progress database file"`
	BucketDest  string `long:"bucket-dest" description:"Copy files to the specified bucket"`
}

func (f *flags) logger() (*logrus.Logger, error) {
	var (
		logOutput io.Writer = nil
		err       error
	)
	if f.LogFile == "" || f.LogFile == "-" {
		logOutput = os.Stderr
	} else {
		logOutput, err = os.OpenFile(f.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, fmt.Errorf("cannot open log file: %w", err)
		}
	}
	logger := &logrus.Logger{
		Out:       logOutput,
		Formatter: new(logrus.TextFormatter),
	}
	switch f.LogLevel {
	case "debug":
		logger.Level = logrus.DebugLevel
	case "info":
		logger.Level = logrus.InfoLevel
	case "warn":
		logger.Level = logrus.WarnLevel
	case "error":
		logger.Level = logrus.ErrorLevel
	default:
		return nil, ErrInvalidLogLevel
	}
	return logger, nil
}

func (f *flags) newLister() (*qiniu_sdk.Lister, string, error) {
	cfg, err := qiniu_sdk.Load(f.ConfigFile)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse config file: %w", err)
	}
	return qiniu_sdk.NewLister(cfg), cfg.Bucket, nil
}

func (f *flags) db(logger gorm_logger.Writer) (*gorm.DB, error) {
	dbLoggerConfig := gorm_logger.Config{
		SlowThreshold: 200 * time.Millisecond,
		LogLevel:      gorm_logger.Warn,
	}
	switch f.LogLevel {
	case "debug", "info":
		dbLoggerConfig.LogLevel = gorm_logger.Info
	case "warn":
		dbLoggerConfig.LogLevel = gorm_logger.Warn
	case "error":
		dbLoggerConfig.LogLevel = gorm_logger.Error
	}
	dbLogger := gorm_logger.New(logger, dbLoggerConfig)

	_, err := os.Stat(f.DbFile)
	if os.IsNotExist(err) {
		f.MigrateDb = true
	}

	database, err := gorm.Open(sqlite.Open(f.DbFile), &gorm.Config{Logger: dbLogger})
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}
	if f.MigrateDb {
		if err = db.AutoMigrateTables(database); err != nil {
			return nil, fmt.Errorf("failed to init database file: %w", err)
		}
	}
	return database, nil
}

func (f *flags) parseBy(parser *goflags.Parser, args []string) (cmd cmds.BaseCmd, err error) {
	if _, err = parser.ParseArgs(args); err != nil {
		return
	}
	lister, bucket, err := f.newLister()
	if err != nil {
		return
	}
	logger, err := f.logger()
	if err != nil {
		return
	}
	database, err := f.db(logger)
	if err != nil {
		return
	}
	cmd.Bucket = bucket
	cmd.Lister = lister
	cmd.Logger = logger
	cmd.Concurrency = f.Concurrency
	cmd.Database = database

	qiniu_sdk.SetLogger(logger)
	return
}

func ParseFlags(ctx context.Context, args []string) (cmds.Command, *goflags.Parser, error) {
	var f flags
	parser := goflags.NewParser(&f, goflags.Default)
	if len(args) < 1 {
		return nil, parser, CmdError{err: ErrEmptyCmd}
	}
	cmd := args[0]
	args = args[1:]

	switch cmd {
	case cmds.StatCmdName:
		if len(args) < 1 {
			return nil, parser, CmdError{err: ErrEmptyOptions}
		}
		inputFilePath := args[0]
		keys, err := input_file.ParseKeys(inputFilePath)
		if err != nil {
			return nil, parser, InputFileParseError{err: err}
		}
		args = args[1:]
		baseCmd, err := f.parseBy(parser, args)
		if err != nil {
			return nil, parser, err
		}
		cmd := cmds.StatCmd{BaseCmd: baseCmd}
		if err = cmd.CreateNewJob(ctx, keys, f.BatchSize); err != nil {
			return nil, parser, DatabaseError{err: err}
		}
		return &cmd, parser, nil
	case cmds.CopyCmdName:
		if len(args) < 1 {
			return nil, parser, CmdError{err: ErrEmptyOptions}
		}
		inputFilePath := args[0]
		var keyPairs []cmds.CopyKeys
		{
			pairs, err := input_file.ParsePairs(inputFilePath)
			if err != nil {
				return nil, parser, InputFileParseError{err: err}
			}
			keyPairs = make([]cmds.CopyKeys, 0, len(pairs))
			for _, pair := range pairs {
				keyPairs = append(keyPairs, cmds.CopyKeys{FromKey: pair.Left, ToKey: pair.Right})
			}
		}
		args = args[1:]
		baseCmd, err := f.parseBy(parser, args)
		if err != nil {
			return nil, parser, err
		}
		cmd := cmds.CopyCmd{BaseCmd: baseCmd}
		if err = cmd.CreateNewJob(ctx, f.BucketDest, keyPairs, f.BatchSize); err != nil {
			return nil, parser, DatabaseError{err: err}
		}
		return &cmd, parser, nil
	case cmds.DeleteCmdName:
		if len(args) < 1 {
			return nil, parser, CmdError{err: ErrEmptyOptions}
		}
		inputFilePath := args[0]
		keys, err := input_file.ParseKeys(inputFilePath)
		if err != nil {
			return nil, parser, InputFileParseError{err: err}
		}
		args = args[1:]
		baseCmd, err := f.parseBy(parser, args)
		if err != nil {
			return nil, parser, err
		}
		cmd := cmds.DeleteCmd{BaseCmd: baseCmd}
		if err = cmd.CreateNewJob(ctx, keys, f.BatchSize); err != nil {
			return nil, parser, DatabaseError{err: err}
		}
		return &cmd, parser, nil
	case cmds.JobsCmdName:
		var jobId *uint64 = nil
		if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
			jobIdNum, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, parser, CmdError{err: fmt.Errorf("failed to parse job id: %w", err)}
			}
			jobId = &jobIdNum
		}
		baseCmd, err := f.parseBy(parser, args)
		if err != nil {
			return nil, parser, err
		}
		return &cmds.JobsCmd{BaseCmd: baseCmd, JobId: jobId, OnlyFailed: f.OnlyFailed}, parser, nil
	case cmds.ContinueCmdName:
		if len(args) < 1 {
			return nil, parser, CmdError{err: ErrEmptyOptions}
		}
		jobId, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, parser, CmdError{err: fmt.Errorf("failed to parse job id: %w", err)}
		}
		args = args[1:]
		baseCmd, err := f.parseBy(parser, args)
		if err != nil {
			return nil, parser, err
		}
		return &cmds.ContinueCmd{BaseCmd: baseCmd, JobId: jobId}, parser, nil
	default:
		return nil, parser, CmdError{err: ErrUnknownCmd}
	}
}
