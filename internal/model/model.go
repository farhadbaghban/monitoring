package model

import "time"

// Sensors is the canonical order used by the API, storage and UI.
var Sensors = []string{"cpu", "mem", "disk", "disk_io", "net_in", "net_out", "fan"}

// Sample is the JSON body accepted by POST /v1/ingest. Pointers distinguish a
// missing sensor from a legitimate zero.
type Sample struct {
	DeviceID int      `json:"device_id"`
	TS       string   `json:"ts"`
	CPU      *float64 `json:"cpu"`
	Mem      *float64 `json:"mem"`
	Disk     *float64 `json:"disk"`
	DiskIO   *float64 `json:"disk_io"`
	NetIn    *float64 `json:"net_in"`
	NetOut   *float64 `json:"net_out"`
	Fan      *float64 `json:"fan"`
}

// WireSample is the Kafka payload. Sensor values are percent x 100; nil means
// the incoming sensor value was null or absent.
type WireSample struct {
	DeviceID int32  `json:"device_id"`
	TS       int64  `json:"ts"` // unix microseconds
	CPU      *int16 `json:"cpu"`
	Mem      *int16 `json:"mem"`
	Disk     *int16 `json:"disk"`
	DiskIO   *int16 `json:"disk_io"`
	NetIn    *int16 `json:"net_in"`
	NetOut   *int16 `json:"net_out"`
	Fan      *int16 `json:"fan"`
}

// Values returns the seven sensor values in canonical order.
func (w WireSample) Values() [7]*int16 {
	return [7]*int16{w.CPU, w.Mem, w.Disk, w.DiskIO, w.NetIn, w.NetOut, w.Fan}
}

func (w WireSample) Time() time.Time {
	return time.UnixMicro(w.TS).UTC()
}

// Device is a row of the devices table.
type Device struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}
