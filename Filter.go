package main

type filterFunc func(interface{}) bool

//The RDD's data must be an array
//The filterFunc is a predicate for if any value inside an array should be included in the new array
func (rdd *RDD) filter(filterFunction filterFunc) *RDD {
	newArr := make([]interface{}, 0)
	for _, v := range rdd.data {
		if filterFunction(v) {
			newArr = append(newArr, v)
		}
	}
	return &RDD{newArr, rdd.numPartitions, rdd, nil, "filter", filterFunction}
}

//FilterIntLessThan35 will return true if the integer is less than 35
//Mainly for testing and to give users an idea on how make their own filter predicate
func FilterIntLessThan35(data interface{}) bool {
	typeCheck(data, 1)
	return data.(int) < 35
}
