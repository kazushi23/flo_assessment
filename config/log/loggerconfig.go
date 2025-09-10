package log

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"time"
)

var (
	Logger  *zap.Logger
	String  = zap.String
	Any     = zap.Any
	Int     = zap.Int
	Float32 = zap.Float32
)

// logpath is the log file path
// loglevel is the log level
func InitLogger(logpath string, loglevel string) {
	// Log file splitting
	pathname := fmt.Sprintf("./logs/%v.log", time.Now().Format("2006-01-02_15"))
	hook := lumberjack.Logger{
		Filename:   pathname, // Log file path, default is os.TempDir()
		MaxSize:    1,        // Each log file saves 1MB, default is 100MB
		MaxBackups: 30,       // Keep 30 backups, default is unlimited
		MaxAge:     30,       // Keep for 30 days, default is unlimited
		Compress:   true,     // Whether to compress, default is not to compress
	}
	write := zapcore.AddSync(&hook)
	// Set log level
	// debug can log info, debug, warn
	// info level can log warn, info
	// warn can only log warn
	// debug -> info -> warn -> error
	var level zapcore.Level
	switch loglevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "error":
		level = zap.ErrorLevel
	case "warn":
		level = zap.WarnLevel
	default:
		level = zap.InfoLevel
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "linenum",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // Lowercase encoder
		// EncodeTime:     zapcore.ISO8601TimeEncoder,     // ISO8601 UTC time format
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.FullCallerEncoder,      // Full path encoder
		EncodeName:     zapcore.FullNameEncoder,
	}
	// Set log level
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(level)

	var writes = []zapcore.WriteSyncer{write}
	// If in development environment, also output to console
	if level == zap.DebugLevel {
		writes = append(writes, zapcore.AddSync(os.Stdout))
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		// zapcore.NewJSONEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(writes...), // Print to console and file
		// write,
		level,
	)

	// Enable development mode, stack tracing
	caller := zap.AddCaller()
	// Enable file and line number
	development := zap.Development()
	// Set initialization fields, e.g., add a server name
	// filed := zap.Fields(zap.String("application", "test-gin"))
	//// Construct the logger
	// Logger = zap.New(core, caller, development, filed)
	Logger = zap.New(core, caller, development)
	Logger.Info("Logger init success")
}
