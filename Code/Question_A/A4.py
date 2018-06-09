#!/usr/bin/env python3

# Python Script For Question A4

import pyspark as pys
from pyspark.sql import SparkSession

ss = SparkSession.builder.master("local").appName("Question A4").getOrCreate()
sparkC = pys.SparkContext()

file_en = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.en")
file_en.cache()
file_bg = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.bg")
file_en.cache()

def swap_key_values(tf):
        return tf.map(lambda tf: (tf[1], tf[0]))

def convert_case(tf):
        return tf.map(lambda x: x.lower())

# Convert To Lower Case
file_en = convert_case(file_en)
file_bg = convert_case(file_bg)

# Zipping
file_en = file_en.zipWithIndex()
file_bg = file_bg.zipWithIndex()

# Swap Key Values
file_en = swap_key_values(file_en)
file_bg = swap_key_values(file_bg)

# Merging
file_merged = file_en.join(file_bg)
file_merged = file_merged.sortBy(lambda a: a[0])

# Same No of Words
smow = file_merged.filter(lambda x: len(x[1][0].split())==len(x[1][1].split()))

lines_en = smow.map(lambda a: a[1][0])
lines_bg = smow.map(lambda a: a[1][1])

tokens_lines_en = lines_en.map(lambda a: a.split())
tokens_lines_bg = lines_bg.map(lambda a: a.split())

# Construct Token Tuples
tokenTuple = tokens_lines_en.zip(tokens_lines_bg)

# Construct Word Pair Tuples
wordpairTuple = tokenTuple.map(lambda a: list(zip(a[0],a[1])))

# Word Pair Fequency Flat Map
wordPairFrequencies = wordpairTuple.flatMap(lambda line: line).map(lambda word_pair: (word_pair, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: -a[1])

# 20 Values of Word Pair Frequencies From The Top
wordpairFrequencies.take(20)
