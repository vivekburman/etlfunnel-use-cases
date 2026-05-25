package client_destinationwrite_4

// Zomato Platform Order Intelligence — destinationwrite_4: Write Tuning (STEP-32)
//
// Dynamically adjusts the Elasticsearch bulk-index batch size based on time-of-day.
//
// Modes:
//
//   Speedify (off-peak 22:00–09:00 IST):
//     RecordsPerBatch = 5000
//
//   Slowify (peak hours 09:00–22:00 IST):
//     RecordsPerBatch = 50
//
// DestinationWriteRule is called once at pipeline init. It returns a
// DestinationWriteTune whose UserDefinedCheckFunc is fired on every CheckInterval
// tick. The closure calls State.SetDestinationWriteBatchSize when the mode changes.

import (
	"etlfunnel/execution/models"
	"sync"
	"time"
)

const (
	peakStartHour = 9  // 09:00 IST
	peakEndHour   = 22 // 22:00 IST
	istOffsetMin  = 5*60 + 30 // IST = UTC+5:30 in minutes

	batchSizeTurbo    = 5000 // speedify
	batchSizeThrottle = 50   // slowify
	checkInterval     = 30 * time.Second
)

var (
	lastMode   string
	lastModeMu sync.Mutex
)

func DestinationWriteRule(param *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	mode := currentMode()

	lastModeMu.Lock()
	lastMode = mode
	lastModeMu.Unlock()

	return &models.DestinationWriteTune{
		RecordsPerBatch: uint(batchSizeForMode(mode)),
		CheckInterval:   checkInterval,
		UserDefinedCheckFunc: func(props *models.CustomDestinationWriteCheckProps) error {
			newMode := currentMode()

			lastModeMu.Lock()
			changed := newMode != lastMode
			if changed {
				lastMode = newMode
			}
			lastModeMu.Unlock()

			if !changed {
				return nil
			}

			props.State.SetDestinationWriteBatchSize(batchSizeForMode(newMode))
			return nil
		},
	}, nil
}

func currentMode() string {
	now := time.Now().UTC()
	istMinutes := now.Hour()*60 + now.Minute() + istOffsetMin
	istHour := (istMinutes / 60) % 24
	if istHour >= peakStartHour && istHour < peakEndHour {
		return "slowify"
	}
	return "speedify"
}

func batchSizeForMode(mode string) int {
	if mode == "slowify" {
		return batchSizeThrottle
	}
	return batchSizeTurbo
}
