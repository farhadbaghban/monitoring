package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/adanic/monitoring/internal/model"
)

// Tier describes one level of the raw -> 1m -> 5m -> 1h -> 4h hierarchy.
type Tier struct {
	Name      string
	Source    string
	Bucket    time.Duration
	Retention time.Duration
}

var tiers = []Tier{
	{Name: "raw", Source: "metrics_raw", Bucket: time.Second, Retention: 7 * 24 * time.Hour},
	{Name: "1m", Source: "metrics_1m", Bucket: time.Minute, Retention: 30 * 24 * time.Hour},
	{Name: "5m", Source: "metrics_5m", Bucket: 5 * time.Minute, Retention: 365 * 24 * time.Hour},
	{Name: "1h", Source: "metrics_1h", Bucket: time.Hour, Retention: 5 * 365 * 24 * time.Hour},
	{Name: "4h", Source: "metrics_4h", Bucket: 4 * time.Hour, Retention: 10 * 365 * 24 * time.Hour},
}

var sensorSet = func() map[string]bool {
	m := make(map[string]bool, len(model.Sensors))
	for _, s := range model.Sensors {
		m[s] = true
	}
	return m
}()

// Series is one sensor's min/avg/max/count arrays. A missing bucket has a null
// min/avg/max and count 0 so the chart breaks the line.
type Series struct {
	Min   []*float64 `json:"min"`
	Avg   []*float64 `json:"avg"`
	Max   []*float64 `json:"max"`
	Count []int64    `json:"count"`
}

type QueryResult struct {
	DeviceID      int               `json:"device_id"`
	From          time.Time         `json:"from"`
	To            time.Time         `json:"to"`
	Tier          string            `json:"tier"`
	BucketSeconds int               `json:"bucket_seconds"`
	TS            []int64           `json:"ts"`
	Series        map[string]Series `json:"series"`
}

func chooseTier(from, to time.Time, maxPoints int, now time.Time) (Tier, time.Duration) {
	rng := to.Sub(from)
	idx := 0
	switch {
	case rng <= time.Hour:
		idx = 0
	case rng <= 24*time.Hour:
		idx = 1
	case rng <= 7*24*time.Hour:
		idx = 2
	case rng <= 90*24*time.Hour:
		idx = 3
	default:
		idx = 4
	}
	// Fall back to a coarser tier if the preferred one no longer holds `from`.
	for idx < len(tiers)-1 && now.Sub(from) > tiers[idx].Retention {
		idx++
	}
	// Keep recent ranges up to an hour on raw data and widen their buckets.
	bucket := tiers[idx].Bucket
	if maxPoints > 0 {
		for idx > 0 && idx < len(tiers)-1 && rng/bucket > time.Duration(maxPoints) {
			idx++
			bucket = tiers[idx].Bucket
		}
		// An output bucket must contain a whole number of source buckets.
		span := to.Sub(from.UTC().Truncate(bucket))
		units := int64((span-1)/bucket) + 1
		bucket *= time.Duration((units-1)/int64(maxPoints) + 1)
	}
	return tiers[idx], bucket
}

func parseSensors(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return model.Sensors
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if sensorSet[p] && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return model.Sensors
	}
	return out
}

// QueryMetrics selects the cheapest tier that satisfies the range and point
// budget, then re-buckets to the requested interval.
func (s *Store) QueryMetrics(ctx context.Context, deviceID int, from, to time.Time, sensorsRaw string, maxPoints int) (QueryResult, error) {
	now := time.Now().UTC()
	tier, bucket := chooseTier(from, to, maxPoints, now)
	sensors := parseSensors(sensorsRaw)
	start := from.UTC().Truncate(tier.Bucket)
	sourceFrom := from
	if tier.Name != "raw" {
		// Rollups include the whole source bucket at each edge.
		sourceFrom = start
	}

	interval := fmt.Sprintf("%d seconds", int64(bucket/time.Second))
	var inner, outer strings.Builder
	// The count determines whether a sensor has any readings in the bucket.
	// Use zero for NULL aggregates when scanning; those values are omitted when count is zero.
	inner.WriteString("SELECT time_bucket(")
	inner.WriteString(fmt.Sprintf("INTERVAL '%s', ", interval))
	if tier.Name == "raw" {
		inner.WriteString("ts, $4::timestamptz) AS bucket")
		for _, s := range sensors {
			inner.WriteString(", " + s)
		}
		inner.WriteString(" FROM " + tier.Source + " WHERE device_id = $1 AND ts >= $2 AND ts < $3")
		outer.WriteString("SELECT g.bucket")
		for _, s := range sensors {
			outer.WriteString(fmt.Sprintf(", coalesce(min(g.%s)::bigint, 0), coalesce(max(g.%s)::bigint, 0), coalesce(sum(g.%s)::bigint, 0), count(g.%s)::bigint", s, s, s, s))
		}
	} else {
		inner.WriteString("bucket, $4::timestamptz) AS bucket")
		for _, s := range sensors {
			inner.WriteString(fmt.Sprintf(", %s_min, %s_max, %s_sum, %s_cnt", s, s, s, s))
		}
		inner.WriteString(" FROM " + tier.Source + " WHERE device_id = $1 AND bucket >= $2 AND bucket < $3")
		outer.WriteString("SELECT g.bucket")
		for _, s := range sensors {
			outer.WriteString(fmt.Sprintf(", coalesce(min(g.%s_min)::bigint, 0), coalesce(max(g.%s_max)::bigint, 0), coalesce(sum(g.%s_sum)::bigint, 0), coalesce(sum(g.%s_cnt)::bigint, 0)", s, s, s, s))
		}
	}
	outer.WriteString(" FROM (" + inner.String() + ") g GROUP BY g.bucket ORDER BY g.bucket")

	rows, err := s.pool.Query(ctx, outer.String(), deviceID, sourceFrom, to, start)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	type row struct {
		bucket time.Time
		vals   []int64 // 4 per sensor: min,max,sum,count
	}
	var data []row
	for rows.Next() {
		var r row
		r.vals = make([]int64, len(sensors)*4)
		dest := make([]any, 0, 1+len(r.vals))
		dest = append(dest, &r.bucket)
		for i := range r.vals {
			dest = append(dest, &r.vals[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return QueryResult{}, err
		}
		data = append(data, r)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, err
	}

	n := int((to.Sub(start)-1)/bucket) + 1
	res := QueryResult{
		DeviceID:      deviceID,
		From:          from.UTC(),
		To:            to.UTC(),
		Tier:          tier.Name,
		BucketSeconds: int(bucket / time.Second),
		TS:            make([]int64, n),
		Series:        make(map[string]Series, len(sensors)),
	}
	for i := 0; i < n; i++ {
		res.TS[i] = start.Add(time.Duration(i) * bucket).UnixMilli()
	}
	for _, s := range sensors {
		res.Series[s] = Series{
			Min:   make([]*float64, n),
			Avg:   make([]*float64, n),
			Max:   make([]*float64, n),
			Count: make([]int64, n),
		}
	}
	for _, r := range data {
		idx := int(r.bucket.Sub(start) / bucket)
		if idx < 0 || idx >= n {
			continue
		}
		for si, s := range sensors {
			mn := r.vals[si*4+0]
			mx := r.vals[si*4+1]
			sum := r.vals[si*4+2]
			cnt := r.vals[si*4+3]
			ser := res.Series[s]
			ser.Count[idx] = cnt
			if cnt > 0 {
				mnf := float64(mn) / 100
				mxf := float64(mx) / 100
				avg := float64(sum) / float64(cnt) / 100
				ser.Min[idx] = &mnf
				ser.Max[idx] = &mxf
				ser.Avg[idx] = &avg
			}
			res.Series[s] = ser
		}
	}
	return res, nil
}
