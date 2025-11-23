package utils

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type Logger struct {
	logger *log.Logger
}

type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

func NewLogger() *Logger {
	return &Logger{
		logger: log.New(os.Stdout, "", 0),
	}
}

func (l *Logger) log(level, msg string, fields map[string]interface{}) {
	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     level,
		Message:   msg,
		Fields:    fields,
	}
	data, _ := json.Marshal(entry)
	l.logger.Println(string(data))
}

func (l *Logger) Info(msg string, fields map[string]interface{}) {
	l.log("INFO", msg, fields)
}

func (l *Logger) Error(msg string, fields map[string]interface{}) {
	l.log("ERROR", msg, fields)
}

func (l *Logger) Warn(msg string, fields map[string]interface{}) {
	l.log("WARN", msg, fields)
}

func (l *Logger) Debug(msg string, fields map[string]interface{}) {
	l.log("DEBUG", msg, fields)
}