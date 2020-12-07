package main

import "sort"

//A definition for the comparator function that sort takes in
type comparatorFunc func(interface{}, interface{}) bool

//sort the RDD's array using the an input comparator function
//the sorting used stable sort, which keeps the original order of equal elements
//The RDD should contain a list of keyvalue pairs
//this sorts by key
func (rdd *RDD) sort(comparatorFunction comparatorFunc) *RDD {
	typeCheck(rdd.data[0], KeyValuePair{1, 1})
	retArr := make([]interface{}, len(rdd.data))
	copy(retArr, rdd.data)
	sort.SliceStable(retArr, func(i, j int) bool {
		return comparatorFunction(retArr[i].(KeyValuePair).key, retArr[j].(KeyValuePair).key)
	})
	return &RDD{retArr, rdd.numPartitions, rdd, nil, "sort", comparatorFunction}
}

//SortIntByAscending helps sort integers by ascending order
//Comparators should return true if the first input should be before the second element
func SortIntByAscending(i, j interface{}) bool {
	typeCheck(i, 1)
	return i.(int) < j.(int)
}
