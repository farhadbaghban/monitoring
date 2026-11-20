package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type point struct{ x, y float64 }
type line struct {
	label  string
	points []point
}

func report(kind string) error {
	decoder := json.NewDecoder(os.Stdin)
	var result loadResult
	if err := decoder.Decode(&result); err != nil {
		return err
	}
	if len(result.Stages) == 0 {
		return fmt.Errorf("no benchmark stages")
	}
	var body, title string
	switch kind {
	case "write-rate", "chart-latency":
		var a, b line
		var axis string
		if kind == "write-rate" {
			title, axis = "Write rate", "Samples per second"
			a.label, b.label = "HTTP acknowledged", "Persisted, including drain"
		} else {
			title, axis = "One-year chart latency", "Milliseconds"
			a.label, b.label = "Measured p99", "SLO (1,000 ms)"
		}
		for _, s := range result.Stages {
			x := float64(s.Devices)
			if kind == "write-rate" {
				a.points = append(a.points, point{x, s.AckRate})
				if s.Stored != nil {
					b.points = append(b.points, point{x, s.WriteRate})
				}
			} else {
				a.points = append(a.points, point{x, s.ChartP99})
				b.points = append(b.points, point{x, 1000})
			}
		}
		body = plot(title, "Active devices at one sample per second", axis, []line{a, b})
	case "resources":
		type stats struct{ Name, CPUPerc, MemUsage string }
		cpus, memories := map[string][]point{}, map[string][]point{}
		for {
			var s stats
			if err := decoder.Decode(&s); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if s.Name == "" || s.CPUPerc == "" || s.MemUsage == "" {
				continue
			}
			name := s.Name[strings.LastIndex(strings.TrimSuffix(s.Name, "-1"), "-")+1:]
			name = strings.TrimSuffix(name, "-1")
			cpu, err := strconv.ParseFloat(strings.TrimSuffix(s.CPUPerc, "%"), 64)
			if err != nil {
				return err
			}
			mem, err := mib(strings.TrimSpace(strings.SplitN(s.MemUsage, "/", 2)[0]))
			if err != nil {
				return err
			}
			cpus[name] = append(cpus[name], point{float64(len(cpus[name]) + 1), cpu})
			memories[name] = append(memories[name], point{float64(len(memories[name]) + 1), mem})
		}
		if len(cpus) == 0 {
			return fmt.Errorf("no Docker resource samples")
		}
		var names []string
		for name := range cpus {
			names = append(names, name)
		}
		sort.Strings(names)
		var cpu, mem []line
		for _, name := range names {
			cpu = append(cpu, line{name, cpus[name]})
			mem = append(mem, line{name, memories[name]})
		}
		title = "Docker resource usage"
		body = plot("CPU by service", "Docker snapshot number", "CPU % (100% = one logical core)", cpu) + plot("Memory by service", "Docker snapshot number", "MiB", mem)
	default:
		return fmt.Errorf("unknown report %q", kind)
	}
	fmt.Printf(`<!doctype html><html lang="en"><meta charset="utf-8"><title>%s</title>
<style>body{font:15px system-ui;margin:2rem auto;max-width:1080px;padding:0 1rem;color:#142336;background:#f4f8fc}section{background:white;padding:1rem 1.25rem;margin:1rem 0;border:1px solid #d5e0ed;border-radius:12px}svg{width:100%%;height:auto}h1{font-size:1.8rem}p{color:#455971}text{fill:#293e58;font:14px system-ui}ul{display:flex;flex-wrap:wrap;gap:.4rem 1.2rem;padding:0;list-style:none}li::before{content:'●';color:var(--c);margin-right:.4rem}</style>
<h1>%s</h1><p>Highest passing: %d devices · First failing: %s · %ds per stage · Ceiling: %d devices</p>%s</html>
`, html.EscapeString(title), html.EscapeString(title), result.Maximum, failure(result.FirstFailure), result.Seconds, result.Ceiling, body)
	return nil
}

func failure(n int) string {
	if n == 0 {
		return "not reached"
	}
	return strconv.Itoa(n)
}

func mib(value string) (float64, error) {
	for _, unit := range []struct {
		name   string
		factor float64
	}{{"TiB", 1048576}, {"GiB", 1024}, {"MiB", 1}, {"KiB", 1. / 1024}, {"MB", 1e6 / (1024 * 1024)}, {"kB", 1000. / (1024 * 1024)}, {"B", 1. / (1024 * 1024)}} {
		if strings.HasSuffix(value, unit.name) {
			n, err := strconv.ParseFloat(strings.TrimSuffix(value, unit.name), 64)
			return n * unit.factor, err
		}
	}
	return 0, fmt.Errorf("unknown memory value %q", value)
}

func plot(title, xLabel, yLabel string, lines []line) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<section><h2>%s</h2><svg viewBox="0 0 950 360" role="img" aria-label="%s">`, html.EscapeString(title), html.EscapeString(title))
	var highX, highY float64
	for _, l := range lines {
		for _, p := range l.points {
			highX = max(highX, p.x)
			highY = max(highY, p.y)
		}
	}
	highX, highY = max(1, highX), max(1, highY*1.07)
	const left, bottom, width, height = 86., 302., 828., 275.
	for i := 0; i <= 5; i++ {
		x, y := left+width*float64(i)/5, bottom-height*float64(i)/5
		fmt.Fprintf(&b, `<path d="M %.2f %.2f H %.2f M %.2f %.2f V %.2f" stroke="#e0e7ef" fill="none"/><text x="%.2f" y="%.2f" text-anchor="end">%.1f</text><text x="%.2f" y="324" text-anchor="middle">%.0f</text>`, left, y, left+width, x, bottom, bottom-height, left-8, y+5, highY*float64(i)/5, x, highX*float64(i)/5)
	}
	fmt.Fprintf(&b, `<text x="500" y="350" text-anchor="middle">%s</text><text transform="translate(21 160) rotate(-90)" text-anchor="middle">%s</text>`, html.EscapeString(xLabel), html.EscapeString(yLabel))
	colors := []string{"#0b6ec0", "#ad3f18", "#18805f", "#8d4ec6", "#9a740b", "#ba3258", "#414c92", "#287790", "#53652c", "#6b4423"}
	for i, l := range lines {
		color := colors[i%len(colors)]
		fmt.Fprintf(&b, `<polyline stroke="%s" stroke-width="2" fill="none" points="`, color)
		for _, p := range l.points {
			fmt.Fprintf(&b, "%.2f,%.2f ", left+width*p.x/highX, bottom-height*p.y/highY)
		}
		fmt.Fprint(&b, `"/>`)
		for _, p := range l.points {
			fmt.Fprintf(&b, `<circle cx="%.2f" cy="%.2f" r="3" fill="%s"><title>%s: %.2f at %.0f</title></circle>`, left+width*p.x/highX, bottom-height*p.y/highY, color, html.EscapeString(l.label), p.y, p.x)
		}
	}
	fmt.Fprint(&b, "</svg><ul>")
	for i, l := range lines {
		fmt.Fprintf(&b, `<li style="--c:%s">%s</li>`, colors[i%len(colors)], html.EscapeString(l.label))
	}
	fmt.Fprint(&b, "</ul></section>")
	return b.String()
}
