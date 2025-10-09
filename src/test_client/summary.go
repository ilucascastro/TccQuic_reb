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

// SummaryLogger escreve métricas agregadas de sessão, como Join latency,
// em um CSV separado para facilitar análise posterior.
type SummaryLogger struct {
    fileWriter *bufio.Writer
    mutex      sync.Mutex
    file       fs.File
}

func NewSummaryLogger(path string) *SummaryLogger {
    const header string = "join_latency_ms\n"

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

// LogJoinLatency grava a Join latency em milissegundos.
func (s *SummaryLogger) LogJoinLatency(d time.Duration) {
    s.mutex.Lock()
    defer s.mutex.Unlock()

    row := fmt.Sprintf("%d\n", d.Milliseconds())
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

