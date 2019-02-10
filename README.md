# Finding-Frequent-Itemset
Implemented SON Algorithm to find frequent itemset

## Version
Scala: 2.11 Spark: 2.2.1

## Command to run
spark-submit –class Son Son.jar

## SON Algorithm
**Phase 1**
- Discovering frequent itemsets using Apriori algorithm. Run the function "apriori" to get all the frequent singletons.
- The support threshold is calculated using ratio. support * (number of baskets in each partition/total number of baskets) 
- Produced candidate itemsets by sending all the singletons to function "combination" to get frequent pairs triples, etc. 

**Phase 2**
- In the second phase, the input is candidate itemsets from previous stage.
- Calculated the occurence of each itemset in each partition and combine the occurence by doing reduceByKey
- Filter out real frequent itemsets using given support threshold


