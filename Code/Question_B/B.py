#!/usr/bin/env python3

# Python Script For All Tasks in Question B

import pyspark as pys
sparkC = pys.SparkContext()

from pyspark.sql import SparkSession
ss = SparkSession.builder.appName("Python - Spark SQL basics").config("Spark.Conf.Options", "random-value").getOrCreate()

Data_A = ss.read.csv("/home/ubuntu/DATA/data_pp.csv",header=True)
Data_A.createOrReplaceTempView("people")

# Task B1.1
Data_A = ss.sql("SELECT DiffMeanHourlyPercent, EmployerName FROM people where DiffMeanHourlyPercent>=50 order by DiffMeanHourlyPercent desc")
Data_A.show()

Data_A = ss.sql("SELECT DiffMeanHourlyPercent, EmployerName FROM people where DiffMeanHourlyPercent>='0' and DiffMeanHourlyPercent<'10' order by DiffMeanHourlyPercent asc")
Data_A.show()

# Task B1.2
Data_A = ss.sql("SELECT sum(DiffMeanHourlyPercent)/10491 FROM people")
Data_A.show()

# Task B1.3
Data_A = ss.sql("SELECT sum(DiffMeanHourlyPercent)/10491 FROM people")
Data_A.toPandas().to_csv('/home/ubuntu/DATA/Task_B_1_3.csv')

# Task B1.4
ss.sql("SELECT count(*) FROM people").show()

Data_A = ss.sql("SELECT count(*)/10491 FROM people where DiffMeanHourlyPercent<'0'")
Data_A.show()

# Task B2.1
Data_B = Data_A.join(ss.broadcast(sic_codes_file), ( Data_A.SicCodes == sic_codes_file.Min) & (Data_A.SicCodes != 1) )
Data_B.show()

# Task B2.2
Data_B = ss.sql("select sum(DiffMeanHourlyPercent),count(Industry),Industry from people group by Industry order by Industry desc")
Data_B.show()

Data_B = ss.sql("select sum(DiffMeanHourlyPercent)/count(Industry),Industry from people group by Industry order by Industry desc")
Data_B.show()

# Task B2.3
Data_B = ss.sql("select sum(DiffMedianHourlyPercent)/count(Industry),Industry from people group by Industry order by Industry desc")
Data_B.show()

Data_B = ss.sql("select sum(DiffMeanBonusPercent)/count(Industry),Industry from people group by Industry order by Industry desc")
Data_B.show()

Data_B = ss.sql("select sum(DiffMedianBonusPercent)/count(Industry),Industry from people group by Industry order by Industry desc")
Data_B.show()
