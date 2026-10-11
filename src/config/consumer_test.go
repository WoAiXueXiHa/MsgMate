package config

import "testing"

func TestConsumerValidation(t *testing.T) {
	if err := (ConsumerConfig{}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []ConsumerConfig{{Workers: -1}, {Workers: 33}, {LowBatch: 1001}, {RetryIntervalMS: -1}, {IdleMinMS: 501, IdleMaxMS: 500}, {ChannelRatePerSecond: -1}, {KafkaBatchWaitMS: 1001}} {
		if c.Validate() == nil {
			t.Fatalf("accepted invalid configuration %+v", c)
		}
	}
}
