package main

import (
	"strings"
)

//The return value of flatMapFunc's should be an array
//[]int cannot be casted as []interface{}, which is why we must use interface{} as the return type
type flatMapFunc func(interface{}) []interface{}

//flatMap takes in a custom function and applies it to the RDD and returns a new RDD with the
//mapping applied to it and flattens the results
func (rdd *RDD) flatMap(flatMapFunction flatMapFunc) *RDD {
	newArr := make([][]interface{}, len(rdd.data))
	for i, v := range rdd.data {
		newArr[i] = flatMapFunction(v)
	}
	var tempArray []interface{}
	for i, v := range newArr {
		if i == 0 {
			tempArray = v
			continue
		}
		tempArray = append(tempArray, v...)
	}
	return &RDD{tempArray, rdd.numPartitions, rdd, nil, "flatMap", flatMapFunction}
}

//FlatMapStringSplitWhiteSpace takes in a string and splits it up by whitespace (returns []string)
//Used for testing and for users to get an idea of how to make their own flatMap function
//Use InterfaceSlice to make an array of interfaces
func FlatMapStringSplitWhiteSpace(data interface{}) []interface{} {
	typeCheck(data, "")
	return InterfaceSlice(strings.Fields(data.(string)))
}
