package main

import (
	"encoding/json"
	"fmt"
)

const (
	maxConcurrency = 1024
	maxBatchSize   = 10000
)

// TableTarget  table target
type TableTarget struct {
	Gateway string `toml:"gateway" json:"gateway"`
	DB      string `toml:"db" json:"db"`
	Table   string `toml:"table" json:"table"`
}

// Config config
type Config struct {
	Concurrency int         `toml:"concurrency" json:"concurrency"`
	BatchSize   uint64      `toml:"batch_size" json:"batch_size"`
	Src         TableTarget `toml:"src" json:"src"`
	Dest        TableTarget `toml:"dest" json:"dest"`
}

// Validate validate table target
func (t *TableTarget) Validate() error {
	if t.Gateway == "" {
		return fmt.Errorf("invalid gateway address: %s", t.Gateway)
	}
	if t.DB == "" {
		return fmt.Errorf("invalid db name: %s", t.DB)
	}
	if t.Table == "" {
		return fmt.Errorf("invalid table name: %s", t.Table)
	}
	return nil
}

// Validate valiate config
func (c *Config) Validate() error {
	if c.Concurrency <= 0 || c.Concurrency > maxConcurrency {
		return fmt.Errorf("invalid concurrency: %d", c.Concurrency)
	}
	if c.BatchSize <= 0 || c.BatchSize > maxBatchSize {
		return fmt.Errorf("invalid batch size: %d", c.BatchSize)
	}
	if err := c.Src.Validate(); err != nil {
		return fmt.Errorf("invalid src table target: %v", err)
	}
	if err := c.Dest.Validate(); err != nil {
		return fmt.Errorf("invalid dest table target: %v", err)
	}
	return nil
}

func (c *Config) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}
