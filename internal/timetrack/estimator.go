// Package timetrack tracks the progress and estimates completion of a task.
package timetrack

import (
	"time"

	"github.com/kopia/kopia/internal/clock"
)

// Estimator estimates the completion of a task.
type Estimator struct {
	startTime time.Time
}

// Timings returns task progress and timings.
type Timings struct {
	PercentComplete  float64
	EstimatedEndTime time.Time
	Remaining        time.Duration
	SpeedPerSecond   float64
}

// Estimate estimates the completion of a task given the current progress as indicated
// by ration of completed/total.
func (v Estimator) Estimate(completed, total float64) (Timings, bool) {
	now := time.Now() //nolint:forbidigo
	elapsed := now.Sub(v.startTime)

	if elapsed > 1*time.Second && total > 0 && completed > 0 {
		completedRatio := completed / total
		if completedRatio > 1 {
			completedRatio = 1
		}

		if completedRatio < 0 {
			completedRatio = 0
		}

		predictedSeconds := elapsed.Seconds() / completedRatio
		predictedEndTime := v.startTime.Add(time.Duration(predictedSeconds) * time.Second)

		dt := predictedEndTime.Sub(clock.Now()).Truncate(time.Second)
		if dt < 0 {
			dt = 0
		}

		return Timings{
			PercentComplete:  100 * completed / total,
			EstimatedEndTime: now.Add(dt),
			Remaining:        dt,
			SpeedPerSecond:   completed / elapsed.Seconds(),
		}, true
	}

	return Timings{}, false
}

// Completed computes the duration and speed (total per second).
func (v Estimator) Completed(total float64) (totalTime time.Duration, speed float64) {
	dur := time.Since(v.startTime) //nolint:forbidigo
	if dur <= 0 {
		return 0, 0
	}

	return dur, total / dur.Seconds()
}

// Start returns an Estimator object.
func Start() Estimator {
	return Estimator{startTime: time.Now()} //nolint:forbidigo
}
