package mr

import "time"

type Task struct {
	Id int
	Files []string
	NReduce int
	TaskType string
	Start time.Time
	Status string
}
