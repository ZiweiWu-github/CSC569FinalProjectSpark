package main

import (
	"fmt"
	"os"
	"strconv"
)

type mapFunc func(interface{}) interface{}

//myMap takes in a custom map function that will be applied to the RDD and return an RDD with the mapped data
//the function is called "myMap" because "map" was already taken by Go and cannot be used as a function name
func (rdd *RDD) myMap(mapFunction mapFunc) *RDD {
	newArr := make([]interface{}, len(rdd.data))
	for i, v := range rdd.data {
		newArr[i] = mapFunction(v)
	}
	return &RDD{newArr, rdd.numPartitions, rdd, nil, "myMap", mapFunction}
}

//Assumes the RDD holds a list of keyvalue pairs
//Maps the value of each pair using the input function
func (rdd *RDD) mapValues(mapFunction mapFunc) *RDD {
	typeCheck(rdd.data[0], KeyValuePair{1, 1})
	retArr := make([]interface{}, len(rdd.data))
	for i, v := range rdd.data {
		v2 := v.(KeyValuePair)
		retArr[i] = KeyValuePair{v2.key, mapFunction(v2.value)}
	}
	return &RDD{retArr, rdd.numPartitions, rdd, nil, "mapValues", mapFunction}
}

//MapIntToString maps an integer into a string
func MapIntToString(data interface{}) interface{} {
	//type check
	typeCheck(data, 1)
	return strconv.Itoa(data.(int))
}

//MapIntPlusTwenty adds 20 to an int
func MapIntPlusTwenty(data interface{}) interface{} {
	typeCheck(data, 1)
	return data.(int) + 20
}

//MapStringToInt maps a string into an integer
func MapStringToInt(data interface{}) interface{} {
	typeCheck(data, "")
	number, err := strconv.Atoi(data.(string))
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	return number
}

//MapToPair returns a KeyValuePair where the input is the key and the value is 1
func MapToPair(data interface{}) interface{} {
	return KeyValuePair{data, 1}
}
