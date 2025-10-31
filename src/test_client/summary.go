package test_client

import (
    "bufio"
    "fmt"
    "io/fs"
    "log"
    "os"
    "sync"
    "time"
)

// SummaryLogger escreve métricas agregadas de sessão, como Join latency e
// Segment completion rate, em um CSV separado para facilitar análise posterior.
type SummaryLogger struct {
    fileWriter *bufio.Writer
    mutex      sync.Mutex
    file       fs.File
}

func NewSummaryLogger(path string) *SummaryLogger {
	const header string = "join_latency_ms,segment_completion_rate_percent,segment_completion_rate_fov_percent,stale_bytes_ratio_percent,deadline_miss_rate_fov_percent,deadline_miss_rate_nonfov_percent,fov_hit_rate_delivery_percent,useful_goodput_fov_kbps\n"

    file, err := os.Create(path)
    if err != nil {
        log.Panicf("Failed to open %s: %s\n", path, err)
    }
    fileWriter := bufio.NewWriter(file)

    if _, err := fileWriter.WriteString(header); err != nil {
        log.Panicf("Failed to write to %s: %s\n", path, err)
    }

    s := new(SummaryLogger)
    s.fileWriter = fileWriter
    s.file = file
    return s
}

// LogSession grava uma linha com Join latency, Segment completion rate (%) e Stale bytes ratio (%).
func (s *SummaryLogger) LogSession(joinLatency time.Duration, segmentCompletionRatePercent float64, fovCompletionRatePercent float64, staleBytesRatioPercent float64, deadlineMissRateFOV float64, deadlineMissRateNonFOV float64, fovHitRate float64, usefulGoodputKbps float64) {
    s.mutex.Lock()
    defer s.mutex.Unlock()

	row := fmt.Sprintf("%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n", joinLatency.Milliseconds(), segmentCompletionRatePercent, fovCompletionRatePercent, staleBytesRatioPercent, deadlineMissRateFOV, deadlineMissRateNonFOV, fovHitRate, usefulGoodputKbps)
    if _, err := s.fileWriter.WriteString(row); err != nil {
        log.Panicf("Failed to write: %s\n", err)
    }
}

// LogJoinLatency � mantido por compatibilidade; escreve as taxas como -1.00.
func (s *SummaryLogger) LogJoinLatency(d time.Duration) {
    s.mutex.Lock()
    defer s.mutex.Unlock()

	row := fmt.Sprintf("%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n", d.Milliseconds(), -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0)
    if _, err := s.fileWriter.WriteString(row); err != nil {
        log.Panicf("Failed to write: %s\n", err)
    }
}

func (s *SummaryLogger) Close() {
    s.mutex.Lock()
    s.fileWriter.Flush()
    s.file.Close()
    s.mutex.Unlock()
}
