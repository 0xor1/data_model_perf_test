/*
A very simple logging utility
*/
package golog

import (
	"fmt"
	ct "github.com/daviddengcn/go-colortext"
	"os"
	"time"
)

type level int64

const (
	Info     level = iota
	Warning  level = iota
	Error    level = iota
	Critical level = iota
	Fatal    level = iota
)

type logEntry struct {
	time    time.Time
	level   level
	message string
}

type Log interface {
	Info(format string, a ...interface{})
	Warning(format string, a ...interface{})
	Error(format string, a ...interface{})
	Critical(format string, a ...interface{})
	Fatal(format string, a ...interface{})
}

func NewLog(level level, timeFormat string, chanBufSize int) Log {
	channel := make(chan *logEntry, chanBufSize)
	go func() {
		for le := range channel {
			print(timeFormat, le)
		}
	}()
	return &log{
		level:      level,
		timeFormat: timeFormat,
		channel:    channel,
	}
}

type log struct {
	level      level
	timeFormat string
	channel    chan<- *logEntry
}

func (l *log) log(level level, format string, a ...interface{}) {
	if level >= l.level {
		le := &logEntry{
			level:   level,
			time:    time.Now().UTC(),
			message: fmt.Sprintf(format, a...),
		}
		if level == Fatal {
			print(l.timeFormat, le)
			os.Exit(1)
		}
		l.channel <- le
	}
}

func (l *log) Info(format string, a ...interface{}) {
	l.log(Info, format, a...)
}

func (l *log) Warning(format string, a ...interface{}) {
	l.log(Warning, format, a...)
}

func (l *log) Error(format string, a ...interface{}) {
	l.log(Error, format, a...)
}

func (l *log) Critical(format string, a ...interface{}) {
	l.log(Critical, format, a...)
}

func (l *log) Fatal(format string, a ...interface{}) {
	l.log(Fatal, format, a...)
}

func print(timeFormat string, le *logEntry) {
	var level string
	switch le.level {
	case Info:
		level = "INFO    "
		ct.Foreground(ct.Cyan, false)
	case Warning:
		level = "WARNING "
		ct.Foreground(ct.Yellow, false)
	case Error:
		level = "ERROR   "
		ct.Foreground(ct.Red, false)
	case Critical:
		level = "CRITICAL"
		ct.ChangeColor(ct.Black, false, ct.Red, false)
	case Fatal:
		level = "FATAL   "
		ct.ChangeColor(ct.Black, false, ct.White, false)
	}
	fmt.Println(le.time.Format(timeFormat), level, le.message)
	ct.ResetColor()
}
