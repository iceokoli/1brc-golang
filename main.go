package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

func timer(name string) func() {
	start := time.Now()
	return func() {
		log.Printf("%s took %v \n", name, time.Since(start))
	}
}

func main() {
	lenghtQueueBuffer := flag.Int("buffersize", 1000, "length of message buffer")
	cpuProfile := flag.Bool("cpuprofile", false, "switch on cpu profile")
	blockProfile := flag.Bool("blockprofile", false, "switch on block profile")
	fileName := flag.String("filename", "measurements.txt", "name of the file")
	chunkSize := flag.Int("chunksize", 10, "chunks")

	flag.Parse()

	if *cpuProfile {
		f, err := os.Create("cpu_profile.prof")
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
		f, err := os.Create("block_profile.prof")
		if err != nil {
			log.Fatalln("failed to create block profile file, err=%w", err)
		}
		defer f.Close()
		runtime.SetBlockProfileRate(1)
		defer pprof.Lookup("block").WriteTo(f, 0)
	}

	defer timer("main")()
	aggregates := calculateAggregates(*lenghtQueueBuffer, *chunkSize, *fileName)
	aggregates.presentResults()
}

func calculateAggregates(lengthQueueBuffer int, chunkSize int, fileName string) *Aggregates {
	log.Printf("Calculating aggregates with buffer size %v and chunk size %v", lengthQueueBuffer, chunkSize)
	messages := make(chan []string, lengthQueueBuffer)
	allAggregates := make([]*Aggregates, 0, runtime.NumCPU())
	resultsQueue := make(chan *Aggregates, runtime.NumCPU())

	var wg sync.WaitGroup
	wg.Add(3)
	go readThroughFile(&wg, messages, chunkSize)
	go processLineChunksFromFile(&wg, messages, resultsQueue)
	go accumalateResults(&wg, resultsQueue, &allAggregates)
	wg.Wait()

	mergedAggregates := NewAggregates()
	for _, aggregates := range allAggregates {
		mergedAggregates.Update(aggregates)
	}

	return mergedAggregates
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
	for {
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

func processLineChunksFromFile(wgTop *sync.WaitGroup, messages <-chan []string, resultsQueue chan<- *Aggregates) {
	defer wgTop.Done()

	var wg sync.WaitGroup
	for i := 1; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialAggregates := NewAggregates()
			for chunk := range messages {
				processLineFromChunk(chunk, partialAggregates)
			}
			resultsQueue <- partialAggregates
		}()
	}

	wg.Wait()
	close(resultsQueue)
}

func accumalateResults(wg *sync.WaitGroup, resultsQueue <-chan *Aggregates, allAggregates *[]*Aggregates) {
	defer wg.Done()
	for result := range resultsQueue {
		*allAggregates = append(*allAggregates, result)
	}
}

func processLineFromChunk(chunks []string, aggregates *Aggregates) {
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

		aggregates.Upsert(name, temperature)
	}
}
