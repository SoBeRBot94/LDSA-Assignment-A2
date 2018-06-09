#!/usr/bin/env python3
#python Script For Question A3

import pyspark as pys

sparkC = pys.SparkContext()

# Frequents For English

frequentWords_en = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.en")
frequentsCount_en = frequentWords_en.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: -a[1])
frequentsCount_en.count()
output_en = frequentsCount_en.take(10)
print("Frequent Words List For English : %s \n" % output_en)

# Frequents For Bulgarian

frequentWords_bg = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.bg")
frequentsCount_bg = frequentWords_bg.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: -a[1])
frequentsCount_bg.count()
output_bg = frequentsCount_bg.take(10)
print("Frequent Words List For Bulgarian : %s \n" % output_bg)
