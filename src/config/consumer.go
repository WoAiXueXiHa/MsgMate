package config

import "fmt"

// ConsumerConfig bounds in-flight work. Zero values select conservative defaults.
type ConsumerConfig struct {
	Workers              int `toml:"workers"`
	RetryWorkers         int `toml:"retry_workers"`
	HighBatch            int `toml:"high_batch"`
	MiddleBatch          int `toml:"middle_batch"`
	LowBatch             int `toml:"low_batch"`
	RetryBatch           int `toml:"retry_batch"`
	IdleMinMS            int `toml:"idle_min_ms"`
	IdleMaxMS            int `toml:"idle_max_ms"`
	ErrorBackoffMS       int `toml:"error_backoff_ms"`
	RetryIntervalMS      int `toml:"retry_interval_ms"`
	KafkaBatchWaitMS     int `toml:"kafka_batch_wait_ms"`
	ChannelConcurrency   int `toml:"channel_concurrency"`
	ChannelRatePerSecond int `toml:"channel_rate_per_second"`
}

func (c ConsumerConfig) WithDefaults() ConsumerConfig {
	defaults := []struct {
		p *int
		v int
	}{{&c.Workers, 4}, {&c.RetryWorkers, 1}, {&c.HighBatch, 60}, {&c.MiddleBatch, 30}, {&c.LowBatch, 30}, {&c.RetryBatch, 10}, {&c.IdleMinMS, 50}, {&c.IdleMaxMS, 500}, {&c.ErrorBackoffMS, 500}, {&c.RetryIntervalMS, 1000}, {&c.KafkaBatchWaitMS, 5}, {&c.ChannelConcurrency, 4}}
	for _, d := range defaults {
		if *d.p == 0 {
			*d.p = d.v
		}
	}
	return c
}
func (c ConsumerConfig) Validate() error {
	c = c.WithDefaults()
	if c.Workers < 1 || c.Workers > 32 || c.RetryWorkers < 1 || c.RetryWorkers > 32 || c.ChannelConcurrency < 1 || c.ChannelConcurrency > 64 {
		return fmt.Errorf("consumer concurrency out of range")
	}
	for _, n := range []int{c.HighBatch, c.MiddleBatch, c.LowBatch, c.RetryBatch} {
		if n < 1 || n > 1000 {
			return fmt.Errorf("consumer batch must be 1..1000")
		}
	}
	if c.IdleMinMS < 1 || c.IdleMaxMS < c.IdleMinMS || c.IdleMaxMS > 60000 || c.ErrorBackoffMS < 1 || c.ErrorBackoffMS > 60000 || c.RetryIntervalMS < 1 || c.RetryIntervalMS > 60000 || c.KafkaBatchWaitMS < 1 || c.KafkaBatchWaitMS > 1000 || c.ChannelRatePerSecond < 0 || c.ChannelRatePerSecond > 100000 {
		return fmt.Errorf("invalid consumer timing or channel rate")
	}
	return nil
}
