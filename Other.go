package main

//this file is for functions of RDDs that take in another RDD
//or just actions that don't require a custom function

//this function combines 2 RDDs together
//the 2 RDDs must contain the same data type
func (rdd *RDD) union(otherRdd *RDD) *RDD {
	typeCheck(rdd.data[0], otherRdd.data[0])
	return &RDD{append(rdd.data, otherRdd.data...), rdd.numPartitions, rdd, otherRdd, "union", nil}
}

//join (RDD[(K, V)],RDD[(K, W)]) ⇒ RDD[(K, (V, W))]
func (rdd *RDD) join(otherRdd *RDD) *RDD {
	typeCheck(rdd.data[0], KeyValuePair{1, 1})
	typeCheck(otherRdd.data[0], KeyValuePair{1, 1})
	retArr := make([]interface{}, 0)
	for _, v := range rdd.data {
		v2 := v.(KeyValuePair)
		for _, otherV := range otherRdd.lookup(v2.key) {
			retArr = append(retArr, KeyValuePair{v2.key, KeyValuePair{v2.value, otherV}})
		}
	}
	return &RDD{retArr, rdd.numPartitions, rdd, otherRdd, "join", nil}
}

//cogroup (RDD[(K, V)],RDD[(K, W)]) ⇒ RDD[(K, (Seq[V], Seq[W]))]
func (rdd *RDD) cogroup(otherRDD *RDD) *RDD {
	typeCheck(rdd.data[0], pair(1, 1))
	typeCheck(otherRDD.data[0], pair(1, 1))
	retArr := make([]interface{}, 0)
	newMap := make(map[interface{}]interface{})
	for _, v := range rdd.data {
		newMap[v.(KeyValuePair).key] = 's'
	}
	for _, v := range otherRDD.data {
		newMap[v.(KeyValuePair).key] = 's'
	}
	for k := range newMap {
		retArr = append(retArr, KeyValuePair{k, KeyValuePair{rdd.lookup(k), otherRDD.lookup(k)}})
	}
	return &RDD{retArr, rdd.numPartitions, rdd, otherRDD, "cogroup", nil}
}

//crossProduct (RDD[T],RDD[U]) ⇒ RDD[(T, U)]
//more like cartesian product
func (rdd *RDD) crossProduct(otherRDD *RDD) *RDD {
	retArr := make([]interface{}, 0)
	for _, v := range rdd.data {
		for _, v2 := range otherRDD.data {
			retArr = append(retArr, KeyValuePair{v, v2})
		}
	}
	return &RDD{retArr, rdd.numPartitions, rdd, otherRDD, "crossProduct", nil}
}

//groupByKey() : RDD[(K, V)] ⇒ RDD[(K, Seq[V])]
func (rdd *RDD) groupByKey() *RDD {
	typeCheck(rdd.data[0], pair(1, 1))
	newMap := make(map[interface{}]interface{})
	for _, v := range rdd.data {
		newMap[v.(KeyValuePair).key] = 's'
	}
	retArr := make([]interface{}, 0)
	for k := range newMap {
		retArr = append(retArr, KeyValuePair{k, rdd.lookup(k)})
	}
	return &RDD{retArr, rdd.numPartitions, rdd, nil, "groupByKey", nil}
}

//sample(fraction : Float) : RDD[T] ⇒ RDD[T] (Deterministic sampling)

//this returns the length of the array the RDD holds
func (rdd *RDD) count() int {
	return len(rdd.data)
}

//this returns the array that the RDD holds
func (rdd *RDD) collect() []interface{} {
	return rdd.data
}

//The RDD is expected to contain a list of pairs
//Returns a list of values for whose key matched the user input
func (rdd *RDD) lookup(key interface{}) []interface{} {
	typeCheck(rdd.data[0], KeyValuePair{1, 2})
	retArr := make([]interface{}, 0)
	for _, v := range rdd.data {
		v2 := v.(KeyValuePair)
		if key == v2.key {
			retArr = append(retArr, v2.value)
		}
	}
	return retArr
}

//partitionBy(p : Partitioner[K]) : RDD[(K, V)] ⇒ RDD[(K, V)]

//save(path : String) : Outputs RDD to a storage system, e.g., HDFS
