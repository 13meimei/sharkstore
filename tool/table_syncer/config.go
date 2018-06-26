package main

// TableTarget  table target
type TableTarget struct {
	Gateway string `toml:"gateway" json:"gateway"`
	DB      string `toml:"db" json:"db"`
	Table   string `toml:"table" json:"table"`
}

// Config config
type Config struct {
	Src         TableTarget `toml:"src" json:"src"`
	Dest        TableTarget `toml:"dest" json:"dest"`
	Concurrency int         `toml:"concurrency" json:"concurrency"`
	BatchSize   uint64      `toml:"batch_size" json:"batch_size"`
}
