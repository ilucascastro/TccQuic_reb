package test_client

import (
	"bufio"
	"fmt"
	"io/fs"
	"log"
	"main/src/model"
	"os"
	"sync"
	"time"
)

type StatisticsLogger struct {
	fileWriter *bufio.Writer
	mutex      sync.Mutex
	file       fs.File
}

func NewStatisticsLogger(path string) *StatisticsLogger {
	const header string = "time_ns,segment,tile,priority,latency_ns,timedout,skipped,ok,tp,buffer_s,tile_missing_ratio,in_fov,on_time\n"

	file, err := os.Create(path)
	if err != nil {
		log.Panicf("Failed to open %s: %s\n", path, err)
	}
	fileWriter := bufio.NewWriter(file)

	if _, err := fileWriter.WriteString(header); err != nil {
		log.Panicf("Failed to write to %s: %s\n", path, err)
	}

	s := new(StatisticsLogger)
	s.fileWriter = fileWriter
	s.file = file

	return s
}

func (s *StatisticsLogger) Log(timeFromStart time.Duration,
	r model.VideoPacketRequest, latency time.Duration, timedOut bool,
	skipped bool, ok bool, tp float64, bufferSec float64, tileMissingRatio float64, inFOV bool, onTime bool) {
	s.mutex.Lock()

	row := fmt.Sprintf("%d,%d,%d,%d,%d,%t,%t,%t,%f,%.2f,%.2f,%t,%t\n", timeFromStart.Nanoseconds(),
		r.Segment, r.Tile, r.Priority, latency.Nanoseconds(), timedOut, skipped, ok, tp, bufferSec, tileMissingRatio, inFOV, onTime)

	if _, err := s.fileWriter.WriteString(row); err != nil {
		log.Panicf("Failed to write: %s\n", err)
	}

	s.mutex.Unlock()
}

func (s *StatisticsLogger) Close() {
	s.mutex.Lock()
	s.fileWriter.Flush()
	s.file.Close()
	s.mutex.Unlock()
}
