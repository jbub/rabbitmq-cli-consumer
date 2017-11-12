package domain

import (
	"log"
)

type Job interface {
	Do(worker int, infLogger *log.Logger, errLogger *log.Logger)
}

type JobBuilder interface {
	BuildJob(data []byte) (Job, error)
}
