#!/bin/bash
#Python Script For Question A2

# Make Sure numpy Is Installed

import pyspark as pys
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SparkSession

sparkC = pys.SparkContext()

if __name__ == "__main__":
	ss = SparkSession.builder.appName("Tokenize").getOrCreate()
	sentences = ss.read.csv("/home/ubuntu/DATA/europarl-v7.bg-en.en")
	tk = Tokenizer(inputCol="_c0", outputCol="words")
	words = tk.transform(sentences)
	words.select("words").show()
	words.take(100)
