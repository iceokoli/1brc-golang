# 1 Billion Row challenge in Go
The objective of this challenge is to see how quickly I could process 1 billion lines of data. For more info please see https://github.com/gunnarmorling/1brc

## How to run
First you need to generate the dataset by following the instructions here https://github.com/gunnarmorling/1brc and renaming the file to measurements.txt and copy it into the folder

## My Implementation
First Stage (Concurrent)
- One Go routine 1 which reads the file and sends of lines in chunks (chunksize is configurable). Sends it over a channel to GO routine 2
- GO routine 2 gets the chunks and spins up a GO routine (short lived) to process the chunks into aggregates. So the chunks are processed in parrallel. The results from these are sent to a channel GO routine 3.
- GO routine 3 stores the results in a list

Second stage
- all results are merged and presented.

Total time: 4min 13seconds.

Hardware:
- CPU: Intel Core i7 -1260P 12th generation
- RAM: 32GB
