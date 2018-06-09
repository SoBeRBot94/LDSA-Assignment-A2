#!/usr/bin/env python3

# Python Script For Question A1

import pyspark as pys
sparkC = pys.SparkContext()

lines_en = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.en")
lines_en.cache()

lines_bg = sparkC.textFile("/home/ubuntu/DATA/europarl-v7.bg-en.bg")
lines_bg.cache()

# Count The Number of Lines - English Dataset

no_of_lines_en = lines_en.count()
print("No of Lines in the English Dataset : %s" % no_of_lines_en)

# Count The Number of Lines - Bulgarian Dataset

no_of_lines_bg = lines_bg.count()
print("No of Lines in the Bulgarian Dataset : %s" % no_of_lines_bg)

# Count The Number of Partitions

partitions_en = lines_en.getNumPartitions()

print("No of Partitions are : %s" % partitions_en)
