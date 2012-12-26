package gosphinx

import(
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
)

var(
	LogFile = "/var/log/sphinx.log"
)

/***** Persistent connections *****/
func LogConnError(err error) {
	var s string
	// matching rest of the codes needs typecasting, errno is
	// wrapped on OpError
	if e, ok := err.(*net.OpError); ok {
		s = fmt.Sprintf("%T : %s", e.Err, e.Error())
		// print wrapped error string e.g.
		// "syscall.Errno : resource temporarily unavailable"

		if e.Timeout() {
			s += " TIMEOUT"
		}
		if e.Temporary() {
			// True on timeout, socket interrupts or when buffer is full
			s += " TEMPORARY"
		}

		// specific granular error codes in case we're interested
		switch e.Err {
		case syscall.EAGAIN:
			// timeout
			s += " EAGAIN"
		case syscall.EPIPE:
			// broken pipe (e.g. on connection reset)
			s += " EPIPE"
		default:

		}
	} else {
		// print type of the error
		s = fmt.Sprintf("%T", err)
	
		if err == syscall.EINVAL {
			s += " EINVAL"	// socket is not valid or already closed
		}
		
		if err == io.EOF {
			s += " EOF"
		}
	}
	
	Log(s)
}


func Log(v ...interface{}) {
	logFile, err := os.OpenFile(LogFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	logger := log.New(logFile, "", log.Ldate|log.Ltime)
	logger.Print(v...)
}

func Logf(format string, a ...interface{}) {
	Log(fmt.Sprintf(format, a...))
}
