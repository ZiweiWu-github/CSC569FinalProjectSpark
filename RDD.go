package main

import (
	"fmt"
	"os"
	"reflect"
)

//RDD is an abstraction of data
//All RDD data is assumed to be an array
//for type safety, use makeRDD(<data>) function
type RDD struct {
	data                             []interface{} //An array of interfaces representing the data
	numPartitions                    int           //number of partitions the RDD has
	beforeRDD, otherBeforeRDD        *RDD          //The parent RDDs
	funcAppliedToBeforeRDD           string        //the string representing the function that was applied to the parent
	customUserFuncAppliedToBeforeRDD interface{}   //The custom user function that was passed in (if the RDD function required it, nil otherwise)
}

//Prints out the array inside the RDD
func (rdd *RDD) println() {
	str := "["
	rddLen := len(rdd.data) - 1
	for i, v := range rdd.data {
		v2, okay := v.(KeyValuePair)
		if okay {
			v = fmt.Sprintf("(%v, %v)", v2.key, v2.value)
		}
		if i == rddLen {
			str = str + fmt.Sprintf("%v]", v)
		} else {
			str = str + fmt.Sprintf("%v, ", v)
		}

	}
	fmt.Println(str)
}

//Creates an RDD given an array
//This make make an RDD with a partition size of 1
func makeRDD(data interface{}) *RDD {
	return &RDD{InterfaceSlice(data), 1, nil, nil, "nothing", nil}
}

//To make a RDD with partitions
func makeRDDPartitions(data interface{}, partitions int) *RDD {
	return &RDD{InterfaceSlice(data), partitions, nil, nil, "nothing", nil}
}

//Prints the lineage of the RDDs
//Could be improved for readability
func (rdd *RDD) printRDDLineage() {
	rdd.println()
	if rdd.beforeRDD != nil {
		fmt.Println(rdd.funcAppliedToBeforeRDD + " applied to Parent RDD(s):")
		rdd.beforeRDD.printRDDLineage()
	}
	if rdd.otherBeforeRDD != nil {
		fmt.Println("Input of " + rdd.funcAppliedToBeforeRDD + ": Other RDD(s):")
		rdd.otherBeforeRDD.printRDDLineage()
	}
}

//KeyValuePair simulates pairs since Go does not have them
type KeyValuePair struct {
	key, value interface{}
}

//creates a keyvaluepair struct with ease
func pair(a interface{}, b interface{}) KeyValuePair {
	return KeyValuePair{a, b}
}

//Used to type check
//x should be the data type you want to type check
//y should be an example data type you want
func typeCheck(x interface{}, y interface{}) {
	xreflect := reflect.TypeOf(x)
	yreflect := reflect.TypeOf(y)
	if xreflect != yreflect {
		handleTypeError(x, yreflect.String(), xreflect.String())
	}
}

//Helper function to print out type errors and exits the program
func handleTypeError(data interface{}, expectedType string, actualType string) {
	fmt.Printf("%v is not of type %v, got type %v", data, expectedType, actualType)
	os.Exit(2)
}

//InterfaceSlice takes in any array and casts it into an array of interface{}
func InterfaceSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)

	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}
