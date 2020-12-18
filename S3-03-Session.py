import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F



# Code Snippet 1
#   Initialize the Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("S3-03-Session") \
    .getOrCreate()


timeprovince = spark.read.load("C:\\dev\\com\\tglc\\sparkScala\\data\\Location.csv",format="csv", \
                        sep=",", inferSchema="true", header="true")
timeprovince.show()

#Windowing Function -- Ranking
windowSpec = Window().partitionBy(['province']).orderBy(F.desc('confirmed'))
timeprovince.withColumn("rank",F.rank().over(windowSpec)).show()

#Windowing Function -- Lag Variables
windowSpec = Window().partitionBy(['province']).orderBy('date')
timeprovinceWithLag = timeprovince.withColumn("lag_7",F.lag("confirmed", 7).over(windowSpec))

timeprovinceWithLag.filter(timeprovinceWithLag.date>'2020-03-10').show()

#Windoring Function -- Rolling Aggregation
windowSpec = Window().partitionBy(['province']).orderBy('date').rowsBetween(-6,0)
timeprovinceWithRoll = timeprovince.withColumn("roll_7_confirmed",F.mean("confirmed").over(windowSpec))
timeprovinceWithRoll.filter(timeprovinceWithLag.date>'2020-03-10').show()

#Windoring Function -- Running Total
windowSpec = Window().partitionBy(['province']).orderBy('date').rowsBetween(\
            Window.unboundedPreceding,Window.currentRow)
timeprovinceWithRoll = timeprovince.withColumn("cumulative_confirmed",F.sum("confirmed").over(windowSpec))
timeprovinceWithRoll.filter(timeprovinceWithLag.date>'2020-03-10').show()