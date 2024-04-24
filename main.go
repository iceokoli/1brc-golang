package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Aggregate struct {
	min   float64
	max   float64
	sum   float64
	count float64
	mean  float64
}

func timer(name string) func() {
	start := time.Now()
	return func() {
		log.Printf("%s took %v \n", name, time.Since(start))
	}
}

func main() {
	var lenghtQueueBuffer = flag.Int("buffersize", 1000, "length of message buffer")
	var cpuProfile = flag.Bool("cpuprofile", false, "switch on cpu profile")
	var blockProfile = flag.Bool("blockprofile", false, "switch on block profile")
	var fileName = flag.String("filename", "measurements.txt", "name of the file")
	var chunkSize = flag.Int("chunksize", 10, "chunks")

	flag.Parse()

	if *cpuProfile {
		f, err := os.Create("cpu_profile_first_attempt_" + fmt.Sprint(*lenghtQueueBuffer) + ".prof")
		if err != nil {
			log.Fatalln("failed to create profile file, err=%w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalln("failed to kick off profiler, err=%w", err)
		}

		defer pprof.StopCPUProfile()
	}

	if *blockProfile {
		f, err := os.Create("block_profile_first_attempt_" + fmt.Sprint(*lenghtQueueBuffer) + ".prof")
		if err != nil {
			log.Fatalln("failed to create block profile file, err=%w", err)
		}
		defer f.Close()
		runtime.SetBlockProfileRate(1)
		defer pprof.Lookup("block").WriteTo(f, 0)
	}

	defer timer("main")()
	station_names, aggregates := calculateAggregates(*lenghtQueueBuffer, *chunkSize, *fileName)
	presentResults(station_names, aggregates)
}

func calculateAggregates(lengthQueueBuffer int, chunkSize int, fileName string) (map[string]bool, map[string]Aggregate) {
	log.Printf("Calculating aggregates with buffer size %v and chunk size %v", lengthQueueBuffer, chunkSize)
	stations := make(map[string]bool)
	aggregates := make(map[string]Aggregate)

	var wg sync.WaitGroup
	wg.Add(2)
	messages := make(chan []string, lengthQueueBuffer)

	go readThroughFile(&wg, messages, chunkSize)
	go processLineChunksFromFile(&wg, messages, stations, aggregates)
	wg.Wait()

	return stations, aggregates
}

func readThroughFile(wg *sync.WaitGroup, messages chan<- []string, chunkSize int) {
	defer wg.Done()
	defer func() {
		log.Println("Closing messages channel")
		close(messages)
	}()

	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalln("ERROR: Unable to open the weather_stations.csv file, err=%w", err)
	}
	defer file.Close()

	r := bufio.NewReader(file)

	lines := make([]string, 0, chunkSize)
	j := 0
	for i := 0; i < 100_000_000; i++ {
		rawLine, _, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln("INFO: Unable to read the file, err = %w", err)
		}

		if j < chunkSize {
			lines = append(lines, string(rawLine))
			j++
			continue
		}
		linesToSend := make([]string, chunkSize)
		_ = copy(linesToSend, lines)
		messages <- linesToSend
		lines = lines[:0]
		j = 0
	}
}

func processLineChunksFromFile(wg *sync.WaitGroup, messages <-chan []string, stations map[string]bool, aggregates map[string]Aggregate) {
	defer wg.Done()
	for chunk := range messages {
		processLineFromChunk(chunk, stations, aggregates)
	}
}

func processLineFromChunk(chunks []string, stations map[string]bool, aggregates map[string]Aggregate) {
	for _, message := range chunks {
		if strings.HasPrefix(message, "#") {
			continue
		}

		measurements := strings.Split(message, ";")
		if len(measurements) != 2 {
			log.Fatalln("ERROR: BAD DATA")
		}

		name := measurements[0]
		temperature, err := strconv.ParseFloat(measurements[1], 32)
		if err != nil {
			log.Fatalf("Failed to parse string to float for %s, err = %s", measurements[1], err)
		}

		stations[name] = true
		agg, found := aggregates[name]
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
			aggregates[name] = agg
		}
		if !found {
			aggregates[name] = Aggregate{min: temperature, max: temperature, sum: temperature, count: 1, mean: temperature}
		}
	}
}

func presentResults(stations map[string]bool, aggregates map[string]Aggregate) {
	n_stations := len(stations)

	ordered_stations := make([]string, 0, n_stations)
	for k := range stations {
		ordered_stations = append(ordered_stations, k)
	}
	slices.Sort(ordered_stations)
	log.Printf("Total number of stations: %v", n_stations)
	result := make([]string, 0, n_stations)
	for _, station := range ordered_stations {
		agg := aggregates[station]
		station_result := fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, agg.min, agg.mean, agg.max)
		result = append(result, station_result)
	}

	full_result := strings.Join(result[:10], ", ")
	log.Printf("{%s}", full_result)
}
