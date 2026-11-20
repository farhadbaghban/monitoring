package main

import (
	"reflect"
	"testing"
)

func TestLoadStagesDefaultRamp(t *testing.T) {
	want := []int{100, 300, 700, 1000, 2000, 4000, 8000, 10000}
	if got := loadStages(100, 10000); !reflect.DeepEqual(got, want) {
		t.Fatalf("loadStages(100, 10000) = %v, want %v", got, want)
	}
}

func TestLoadStagesRespectCustomBounds(t *testing.T) {
	tests := []struct {
		name       string
		start, max int
		want       []int
	}{
		{"ceiling inside checkpoints", 100, 750, []int{100, 300, 700, 750}},
		{"custom start", 500, 2500, []int{500, 700, 1000, 2000, 2500}},
		{"start at ceiling", 300, 300, []int{300}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loadStages(tt.start, tt.max); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("loadStages(%d, %d) = %v, want %v", tt.start, tt.max, got, tt.want)
			}
		})
	}
}
