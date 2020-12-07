package main

type reduceFunc func(interface{}, interface{}) interface{}

//This takes in a custom reduce function and returns the reduction
//The RDD should contain an array when calling this
func (rdd *RDD) reduce(reduceFunction reduceFunc) interface{} {
	var value interface{}
	for i, v := range rdd.data {
		if i == 0 {
			value = v
			continue
		}
		value = reduceFunction(value, v)
	}
	return value
}

func (rdd *RDD) reduceByKey(reduceFunction reduceFunc) *RDD {
	typeCheck(rdd.data[0], KeyValuePair{"", ""})
	newMap := make(map[interface{}]interface{})
	for _, v := range rdd.data {
		v2 := v.(KeyValuePair)
		if val, ok := newMap[v2.key]; ok {
			newMap[v2.key] = reduceFunction(val, v2.value)
		} else {
			newMap[v2.key] = v2.value
		}
	}
	newArr := make([]interface{}, len(newMap))
	i := 0
	for k, v := range newMap {
		newArr[i] = KeyValuePair{k, v}
		i = i + 1
	}
	return &RDD{newArr, rdd.numPartitions, rdd, nil, "reduceByKey", reduceFunction}
}

//ReduceIntAdd reduces an array of integers by adding all the values together
func ReduceIntAdd(value1 interface{}, value2 interface{}) interface{} {
	typeCheck(value1, 1)
	return value1.(int) + value2.(int)
}
