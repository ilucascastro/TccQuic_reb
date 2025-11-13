package test_client

import (
	"log"
	"time"

	"main/src/test_client/session"
)

func StartTestClient(serverURL string, serverPort int, parallelism int, baseLatencyMs int) {
	envCfg := ResolveEnvironmentConfig()

	client := NewClient(ClientOptions{
		Pipeline:   envCfg.Pipeline,
		ServerURL:  serverURL,
		ServerPort: serverPort,
	})

	log.Printf("Base latency = %d ms", baseLatencyMs)

	if err := client.Connect(); err != nil {
		log.Printf("failed to connect: %v", err)
		return
	}

	env := session.Environment{
		FOVTracePath:    envCfg.FOVTracePath,
		FOVTraceFPS:     envCfg.FOVTraceFPS,
		StatisticsPath:  envCfg.StatisticsPath,
		SummaryPath:     envCfg.SummaryPath,
		FOVDeliveryPath: envCfg.FOVDeliveryPath,
		FOVGoodputPath:  envCfg.FOVGoodputPath,
	}

	opts := session.Options{
		Parallelism:     parallelism,
		BaseLatency:     time.Duration(baseLatencyMs) * time.Millisecond,
		SegmentDuration: time.Duration(defaultSegmentDurationSeconds) * time.Second,
		FirstSegment:    1,
		LastSegment:     120,
		FirstTile:       100,
		LastTile:        177,
	}

	testSession := session.NewTestSession(client, env, opts)

	if err := testSession.Run(); err != nil {
		log.Printf("test session failed: %v", err)
	}
}
