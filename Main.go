package main

func main() {
	//makeRDD() requires an array as its arguement or it will throw an error

	// //pair testing with reduce by key
	// fmt.Println("Testing with kv pairs and reduceByKey: ")
	// makeRDD([]KeyValuePair{pair("test", 20), pair("test", 40), pair("another", 15)}).reduceByKey(ReduceIntAdd).println()
	// makeRDD([]string{"a", "b", "a", "a", "a", "b", "b", "a"}).myMap(MapToPair).reduceByKey(ReduceIntAdd).println()

	// //filter
	// fmt.Println("\nFiltering an array to only have elements less than 35:")
	// makeRDD([]int{20, 42, 120, 22, 34}).filter(FilterIntLessThan35).println()
	// makeRDD([]int{20, 42, 120, 22, 34}).filter(FilterIntLessThan35).printRDDLineage()

	//flatmap
	// fmt.Println("\nTesting flatmap: splitting an array of strings with whitespace and then bring them back together")
	// makeRDD([]string{"testing, testing, 1, 2, 3", "woo this is for testing", "", "booo"}).flatMap(FlatMapStringSplitWhiteSpace).println()
	// makeRDD([]string{"testing, testing, 1, 2, 3", "woo this is for testing", "", "booo"}).flatMap(FlatMapStringSplitWhiteSpace).printRDDLineage()

	// //mapvalues
	// fmt.Println("\nMapping but only to the values of a pair")
	// makeRDD([]int{1, 2, 3, 4, 5}).myMap(MapToPair).mapValues(MapIntPlusTwenty).println()
	// makeRDD([]int{1, 2, 3, 4, 5}).myMap(MapToPair).mapValues(MapIntPlusTwenty).printRDDLineage()

	// //reduce
	// fmt.Println("\nReduces an RDD and gives the output")
	// fmt.Println(makeRDD([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).reduce(ReduceIntAdd))

	// //lookup
	// fmt.Println("\nlookup: returns an array with all values whose key is the user input")
	// fmt.Println(makeRDD([]KeyValuePair{pair(20, 12), pair(20, 14), pair(33, 22)}).lookup(20))

	// //sort
	// fmt.Println("\nsorts and array of pairs by keys using user made comparator")
	// makeRDD([]int{20, 32, 12, 10, 0, 1}).myMap(MapToPair).sort(SortIntByAscending).println()
	// makeRDD([]int{20, 32, 12, 10, 0, 1}).myMap(MapToPair).sort(SortIntByAscending).printRDDLineage()

	// //union
	// fmt.Println("\nCombines the arrays of the two RDDs")
	// makeRDD([]int{1, 2, 3, 4}).union(makeRDD([]int{5, 6, 7, 8})).println()
	// makeRDD([]int{1, 2, 3, 4}).union(makeRDD([]int{5, 6, 7, 8})).printRDDLineage()

	// //join
	// fmt.Println("\nJoins")
	// makeRDD([]KeyValuePair{pair(0, "John"), pair(1, "Test"), pair(2, "none")}).join(makeRDD([]KeyValuePair{pair(0, "a"), pair(0, "b"), pair(1, "test")})).println()
	makeRDD([]KeyValuePair{pair(0, "John"), pair(1, "Test"), pair(2, "none")}).join(makeRDD([]KeyValuePair{pair(0, "a"), pair(0, "b"), pair(1, "test")})).printRDDLineage()

	// //cogroup
	// fmt.Println("\nCogroup")
	// makeRDD([]KeyValuePair{pair(0, "John"), pair(1, "Test"), pair(2, "none")}).cogroup(makeRDD([]KeyValuePair{pair(0, "a"), pair(0, "b"), pair(1, "test")})).println()
	// makeRDD([]KeyValuePair{pair(0, "John"), pair(1, "Test"), pair(2, "none")}).cogroup(makeRDD([]KeyValuePair{pair(0, "a"), pair(0, "b"), pair(1, "test")})).printRDDLineage()

	// //groupbykey
	// fmt.Println("\ngroupByKey")
	// makeRDD([]KeyValuePair{pair(1, "wooo"), pair(1, "hello"), pair(1, "why"), pair(2, "test")}).groupByKey().println()
	// makeRDD([]KeyValuePair{pair(1, "wooo"), pair(1, "hello"), pair(1, "why"), pair(2, "test")}).groupByKey().printRDDLineage()

	// //crossProduct
	// fmt.Println("\ncrossProduct")
	// makeRDD([]int{1, 2, 3, 4, 5}).crossProduct(makeRDD([]string{"John", "Test"})).println()
	// makeRDD([]int{1, 2, 3, 4, 5}).crossProduct(makeRDD([]string{"John", "Test"})).printRDDLineage()

	//RDDs with partitions
	//makeRDDPartitions([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 3).mapPartitions(MapPartitionHighestNumber).println()

	//error testing
	// makeRDD(2).myMap(MapStringToInt).println()
	//makeRDD(20)
}
