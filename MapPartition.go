package main

//This file is for partitioning maps

//definition of what map partion takes in
type mapParFunc func([]interface{}) []interface{}

//MapPartition uses the custom map function to only map chunks of the RDD's data
func (rdd *RDD) mapPartitions(mapParFunction mapParFunc) *RDD {
	chunkedData := sliceChunk(rdd.data, rdd.numPartitions)
	var retArr []interface{}
	for _, v := range chunkedData {
		retArr = append(retArr, mapParFunction(v)...)
	}
	return &RDD{retArr, rdd.numPartitions, rdd, nil, "mapPartitions", mapParFunction}
}

//helper to divide slice into chunks
func sliceChunk(arr []interface{}, num int) [][]interface{} {
	var divided [][]interface{}
	chunkSize := (len(arr) + num - 1) / num
	for i := 0; i < len(arr); i += chunkSize {
		end := i + chunkSize
		if end > len(arr) {
			end = len(arr)
		}
		divided = append(divided, arr[i:end])
	}
	return divided
}

//MapPartitionHighestNumber is used to demonstrate MapPartition
func MapPartitionHighestNumber(arr []interface{}) []interface{} {
	typeCheck(arr[0], 1)
	var highestnum int
	for i, v := range arr {
		v2 := v.(int)
		if i == 0 {
			highestnum = v2
			continue
		}
		if highestnum < v2 {
			highestnum = v2
		}
	}
	return InterfaceSlice([]int{highestnum})
}
