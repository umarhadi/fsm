package main

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/sirupsen/logrus"
)

func stdLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.InfoLevel)
	l.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	})
	return l
}

// randid generates a random hex id for fsm runs.
func RandID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
