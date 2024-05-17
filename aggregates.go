package main

import (
	"fmt"
	"log"
	"slices"
	"strings"
)

type Aggregate struct {
	min   float64
	max   float64
	sum   float64
	count float64
	mean  float64
}

type Aggregates struct {
	data     map[string]Aggregate
	stations map[string]bool
}

func NewAggregates() *Aggregates {
	return &Aggregates{
		make(map[string]Aggregate),
		make(map[string]bool),
	}
}

func (aggs *Aggregates) RetrieveData(name string) Aggregate {
	return aggs.data[name]
}

func (aggs *Aggregates) RetrieveStations() map[string]bool {
	return aggs.stations
}

func (aggs *Aggregates) Upsert(name string, temperature float64) {
	aggs.stations[name] = true
	agg, found := aggs.data[name]
	if found {
		agg.count += 1
		agg.sum += temperature
		agg.mean = agg.sum / agg.count

		if temperature > agg.max {
			agg.max = temperature
		}

		if temperature < agg.min {
			agg.min = temperature
		}
		aggs.data[name] = agg
	}
	if !found {
		aggs.data[name] = Aggregate{min: temperature, max: temperature, sum: temperature, count: 1, mean: temperature}
	}
}

func (aggs *Aggregates) Update(other *Aggregates) {
	for name := range other.stations {
		aggs.stations[name] = true
	}

	for name, otherAgg := range other.data {
		agg, found := aggs.data[name]
		if !found {
			aggs.data[name] = otherAgg
			continue
		}
		agg.count += otherAgg.count
		agg.sum += otherAgg.sum
		agg.mean = agg.sum / agg.count
		if otherAgg.min < agg.min {
			agg.min = otherAgg.min
		}
		if otherAgg.max > agg.max {
			agg.max = otherAgg.max
		}
		aggs.data[name] = agg
	}
}

func (aggs *Aggregates) presentResults() {
	stations := aggs.RetrieveStations()
	n_stations := len(stations)

	ordered_stations := make([]string, 0, n_stations)
	for k := range stations {
		ordered_stations = append(ordered_stations, k)
	}
	slices.Sort(ordered_stations)
	log.Printf("Total number of stations: %v", n_stations)
	result := make([]string, 0, n_stations)
	for _, station := range ordered_stations {
		agg := aggs.RetrieveData(station)
		station_result := fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, agg.min, agg.mean, agg.max)
		result = append(result, station_result)
	}

	full_result := strings.Join(result[:10], ", ")
	log.Printf("{%s}", full_result)
}
