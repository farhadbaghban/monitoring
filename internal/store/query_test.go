package store

import (
	"testing"
	"time"
)

func TestChooseTier(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		rng  time.Duration
		want string
	}{
		{5 * time.Minute, "raw"},
		{time.Hour, "raw"},
		{6 * time.Hour, "1m"},
		{24 * time.Hour, "1m"},
		{3 * 24 * time.Hour, "5m"},
		{30 * 24 * time.Hour, "1h"},
		{365 * 24 * time.Hour, "4h"},
	}
	for _, c := range cases {
		to := now
		from := to.Add(-c.rng)
		tier, bucket := chooseTier(from, to, 2500, now)
		if tier.Name != c.want {
			t.Errorf("range %s: want %s, got %s", c.rng, c.want, tier.Name)
		}
		if c.rng == time.Hour && bucket != 2*time.Second {
			t.Errorf("one hour with 2500 points: want 2s raw buckets, got %s", bucket)
		}
	}
}

func TestRetentionFallback(t *testing.T) {
	now := time.Now().UTC()
	from := now.Add(-100 * 24 * time.Hour)
	to := from.Add(time.Hour) // a 1h range, but 100 days old
	tier, _ := chooseTier(from, to, 2500, now)
	if tier.Name == "raw" {
		t.Fatal("raw is only retained 7 days; must fall back to a coarser tier")
	}
}

func TestBucketCoarsening(t *testing.T) {
	now := time.Now().UTC()
	from := now.Add(-365 * 24 * time.Hour)
	tier, bucket := chooseTier(from, now, 2500, now)
	if tier.Name != "4h" {
		t.Fatalf("want 4h, got %s", tier.Name)
	}
	if bucket < 4*time.Hour {
		t.Fatalf("bucket %s must be >= 4h", bucket)
	}
}

func TestPointBudgetUsesWholeSourceBuckets(t *testing.T) {
	to := time.Date(2026, 2, 5, 8, 23, 71, 0, time.UTC)
	for _, span := range []time.Duration{59 * time.Minute, 6 * time.Hour, 365 * 24 * time.Hour} {
		from := to.Add(-span)
		tier, bucket := chooseTier(from, to, 7, to)
		start := from.Truncate(tier.Bucket)
		points := (to.Sub(start)-1)/bucket + 1
		if points > 7 || bucket%tier.Bucket != 0 {
			t.Fatalf("%s: tier=%s, bucket=%s, points=%d", span, tier.Name, bucket, points)
		}
	}
}

func TestParseSensors(t *testing.T) {
	if got := parseSensors(""); len(got) != 7 {
		t.Fatalf("default should be all sensors, got %d", len(got))
	}
	if got := parseSensors("cpu,fan"); len(got) != 2 || got[0] != "cpu" || got[1] != "fan" {
		t.Fatalf("unexpected %v", got)
	}
	if got := parseSensors("cpu,bogus"); len(got) != 1 || got[0] != "cpu" {
		t.Fatalf("invalid sensors must be dropped, got %v", got)
	}
}
