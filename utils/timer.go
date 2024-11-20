package utils

import (
	"time"

	"github.com/hako/durafmt"
)

type TimeManager struct {
	StartTime  time.Time
	EndTime    time.Time
	StartBlock uint32
	EndBlock   uint32
}

func NewTimeManager() *TimeManager {
	return &TimeManager{
		StartTime:  time.Now(),
		EndTime:    time.Now(),
		StartBlock: 0,
		EndBlock:   0,
	}
}

func (tm *TimeManager) StartTimer(block uint32) {
	tm.StartTime = time.Now()
	tm.StartBlock = block
}

func (tm *TimeManager) EndTimer(block uint32) {
	tm.EndTime = time.Now()
	tm.EndBlock = block
}

// caluclate how many blocks per second are processed
func (tm *TimeManager) CalculateBPS() uint32 {
	if tm.StartBlock == 0 || tm.EndBlock <= tm.StartBlock {
		return 0
	}
	numberOfBlocks := tm.EndBlock - tm.StartBlock
	seconds := tm.EndTime.Sub(tm.StartTime).Seconds()
	return uint32(float64(numberOfBlocks) / seconds)
}

// calculate how long it will take to process the remaining blocks
func (tm *TimeManager) CalculateRemainingTime(currentBlock uint32, headBlock uint32, blocksPerSecond uint32) string {
	if blocksPerSecond == 0 || currentBlock == 0 || headBlock <= currentBlock {
		return ""
	}
	remainingBlocks := headBlock - currentBlock
	seconds := remainingBlocks / blocksPerSecond
	durationSeconds := (time.Duration(seconds) * time.Second).String()
	duration, err := durafmt.ParseString(durationSeconds)
	if err != nil {
		return ""
	}
	return duration.LimitFirstN(3).String()
}

// calculate how far behind the current block is from the head block
func (tm *TimeManager) CalculateTimeBehind(currentBlock uint32, headBlock uint32) string {
	if currentBlock == 0 || headBlock <= currentBlock {
		return ""
	}
	secondsBehind := (headBlock - currentBlock) / 2
	durationSeconds := (time.Duration(secondsBehind) * time.Second).String()
	duration, err := durafmt.ParseString(durationSeconds)
	if err != nil {
		return ""
	}
	return duration.LimitFirstN(3).String()
}
