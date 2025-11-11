package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
)

func main() {
	log.Println("Starting TinyGraph cluster...")

	// Parse config path from command line
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var cmds []*exec.Cmd

	// Start all shard replicas
	for _, shard := range cfg.Shards {
		for _, replica := range shard.Replicas {
			log.Printf("Starting shard %d replica %d...", shard.ID, replica.ID)
			cmd := exec.Command("go", "run", "cmd/shard/main.go",
				"-config", configPath,
				"-shard-id", fmt.Sprintf("%d", shard.ID),
				"-replica-id", fmt.Sprintf("%d", replica.ID))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
			if err := cmd.Start(); err != nil {
				log.Fatalf("Failed to start shard %d replica %d: %v", shard.ID, replica.ID, err)
			}
			cmds = append(cmds, cmd)
		}
	}

	// Wait for shards to initialize
	log.Println("Waiting for shards to initialize...")
	time.Sleep(2 * time.Second)

	// Start query manager
	log.Println("Starting query manager...")
	qmCmd := exec.Command("go", "run", "cmd/qm/main.go", "-config", configPath)
	qmCmd.Stdout = os.Stdout
	qmCmd.Stderr = os.Stderr
	qmCmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if err := qmCmd.Start(); err != nil {
		log.Fatalf("Failed to start query manager: %v", err)
	}
	cmds = append(cmds, qmCmd)

	// Wait for services to be ready
	log.Println("Waiting for services to start...")
	time.Sleep(3 * time.Second)

	log.Printf("✓ Cluster started successfully!")
	log.Printf("✓ Query manager available at %s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port)
	log.Printf("✓ Running %d shard(s) with %d replica(s) each", len(cfg.Shards), cfg.Replication.ReplicationFactor)
	log.Println("\nPress Ctrl+C to stop the cluster...")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\nShutting down cluster...")
	for _, cmd := range cmds {
		if cmd.Process != nil {
			// Kill process group to ensure all child processes are terminated
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
	log.Println("Cluster stopped.")
}
