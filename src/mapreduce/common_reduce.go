package mapreduce

import (
	"os"
	"encoding/json"
	"fmt"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// Dict for kv from all intermediate files
	// Map[1]["123"] = "1" means that in the first
	// intermediate file, key "123" has value  "1"
	temDict := make(map[string][]string)

	// Prepare input file/Decoder for json/output file
	inFiles := make([]*os.File, nMap)
	Decoders := make([]*json.Decoder, nMap)
	outFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkFile(err)
	Encoder := json.NewEncoder(outFile)


	// Group input files
	for i := range inFiles {
		var err error
		var kv KeyValue

		filename := reduceName(jobName, i, reduceTaskNumber)
		inFiles[i], err = os.Open(filename)
		checkFile(err)

		fmt.Printf("This is reduceTask %d, try to read file %s\n", reduceTaskNumber, filename)

		Decoders[i] = json.NewDecoder(inFiles[i])
		for  {

			if err = Decoders[i].Decode(&kv); nil == err {
				//fmt.Printf("Key/Value:  <%s, %s>\n", kv.Key, kv.Value)

				if _, ok := temDict[kv.Key]; !ok {
					temDict[kv.Key] = make([]string, 0)
				}

				temDict[kv.Key] = append(temDict[kv.Key], kv.Value)

				//j := len(temDict[kv.Key])
				//fmt.Printf("temDict[%s][%d] = %s\n", kv.Key, j-1, temDict[kv.Key][j-1])
			} else {
				break
			}

		}
		inFiles[i].Close()
	}

	//Group key/value
	for key := range temDict {
		totalValue := reduceF(key, temDict[key])
		Encoder.Encode(KeyValue{key, totalValue})
	}
	outFile.Close()
}
