package util

import (
	"log"
	"os"
)

func New(service string) *log.Logger {
	return log.New(os.Stdout, "["+service+"] ", log.LstdFlags|log.Lshortfile)
}
