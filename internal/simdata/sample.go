package simdata

import (
	"math"
	"math/rand"
	"time"
)

type Sample struct {
	DeviceID int    `json:"device_id"`
	TS       string `json:"ts"`
	CPU      int8   `json:"cpu"`
	Mem      int8   `json:"mem"`
	Disk     int8   `json:"disk"`
	DiskIO   int8   `json:"disk_io"`
	NetIn    int8   `json:"net_in"`
	NetOut   int8   `json:"net_out"`
	Fan      int8   `json:"fan"`
}

func Generate(id int, t time.Time, r *rand.Rand) Sample {
	base := func(phase float64, mean float64) int8 {
		v := mean + 18*math.Sin(float64(t.Unix())/900+phase) + r.NormFloat64()*2.5
		return clamp(v)
	}
	return Sample{
		DeviceID: id,
		TS:       t.UTC().Format(time.RFC3339Nano),
		CPU:      base(float64(id)*0.1, 45),
		Mem:      base(float64(id)*0.2+1, 60),
		Disk:     base(float64(id)*0.3+2, 55),
		DiskIO:   base(float64(id)*0.4+3, 30),
		NetIn:    base(float64(id)*0.5+4, 35),
		NetOut:   base(float64(id)*0.6+5, 25),
		Fan:      base(float64(id)*0.7+6, 40),
	}
}

func clamp(v float64) int8 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return int8(math.Round(v))
}
