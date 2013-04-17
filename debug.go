package gosphinx

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
)

var (
	LogFile               = "sphinx.log"     //"/var/log/sphinx.log"
	ErrorLogFile          = "sphinx_err.log" //"/var/log/sphinx_err.log"
	logFile, errorLogFile *os.File
)

func init() {
	var err error

	logFile, err = os.OpenFile(LogFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}

	errorLogFile, err = os.OpenFile(ErrorLogFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
}

func LogConnError(err error) {
	var s string
	if e, ok := err.(*net.OpError); ok {
		s = fmt.Sprintf("%T : %s", e.Err, e.Error())

		if e.Timeout() {
			s += " TIMEOUT"
		}
		if e.Temporary() {
			s += " TEMPORARY" // True on timeout, socket interrupts or when buffer is full
		}

		switch e.Err {
		case syscall.EAGAIN:
			s += " EAGAIN" // timeout
		case syscall.EPIPE:
			s += " EPIPE" // broken pipe (e.g. on connection reset)
		default:
			// Do nothing
		}
	} else {
		s = fmt.Sprintf("%T", err)

		if err == syscall.EINVAL {
			s += " EINVAL" // socket is not valid or already closed
		}

		if err == io.EOF {
			s += " EOF"
		}
	}

	Logf("%s\n", s)
}

func Log(v ...interface{}) {
	logger := log.New(logFile, "", log.Ldate|log.Ltime)
	logger.Print(v...)
}

func LogError(v ...interface{}) {
	logger := log.New(errorLogFile, "", log.Ldate|log.Ltime)
	logger.Print(v...)
}

func Logf(format string, a ...interface{}) {
	Log(fmt.Sprintf(format, a...))
}

func LogErrorf(format string, a ...interface{}) {
	LogError(fmt.Sprintf(format, a...))
}
