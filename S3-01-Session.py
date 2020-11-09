import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Code Snippet 1
#   Initialize the Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("S3-01-Session") \
    .getOrCreate()

print(spark.version)


# Code Snippet 2
#   Read the source tables in Parquet format
		
sales_table = spark.read.parquet('C:\\document++\\TheGiraffeLearningClub\\SparkScala\\data\\DatasetToCompleteTheSixSparkExercises\\sales_parquet')
		
'''
SELECT order_id AS the_order_id,
       seller_id AS the_seller_id,
       num_pieces_sold AS the_number_of_pieces_sold
FROM sales_table
'''
#   Execution Plan and show action in one line 
sales_table_execution_plan = sales_table.select(col("order_id").alias("the_order_id"),
    col("seller_id").alias("the_seller_id"),
    col("num_pieces_sold").alias("the_number_of_pieces_sold")
).show(5, True)

# Code Snippet 3
# Renaming and Adding Columns
'''
SELECT order_id,
       product_id,
       seller_id,
       date,
       num_pieces_sold AS pieces,
       bill_raw_text
FROM sales_table a
'''
sales_table_execution_plan = sales_table. \
    withColumnRenamed("num_pieces_sold", "pieces")

sales_table_execution_plan.show()



# Code Snippet 4
# Simple Aggregation
'''
SELECT product_id,
       SUM(num_pieces_sold) AS total_pieces_sold,
       AVG(num_pieces_sold) AS average_pieces_sold,
       MAX(num_pieces_sold) AS max_pieces_sold_of_product_in_orders,
       MIN(num_pieces_sold) AS min_pieces_sold_of_product_in_orders,
       COUNT(num_pieces_sold) AS num_times_product_sold
FROM sales_table
GROUP BY product_id
'''
sales_table_execution_plan = sales_table.groupBy(
    col("product_id")
).agg(
    sum("num_pieces_sold").alias("total_pieces_sold"),
    avg("num_pieces_sold").alias("average_pieces_sold"),
    max("num_pieces_sold").alias("max_pieces_sold_of_product_in_orders"),
    min("num_pieces_sold").alias("min_pieces_sold_of_product_in_orders"),
    count("num_pieces_sold").alias("num_times_product_sold")
)

sales_table_execution_plan.show()



# Code Snippet 5
# Select Distinct
'''
SELECT DISTINCT seller_id,
       date
FROM sales_table
'''
sales_table_execution_plan = sales_table.select(
    col("seller_id"), col("date")
).distinct()

#   Print Schema
sales_table_execution_plan.show()
sales_table_execution_plan.printSchema()



# Code Snippet 6
# Case When Statement

'''
SELECT seller_id,
       CASE WHEN num_pieces_sold < 30 THEN 'Lower than 30',
            WHEN num_pieces_sold < 60 THEN 'Between 31 and 60'
            WHEN num_pieces_sold < 90 THEN 'Between 61 and 90'
            ELSE 'More than 91' AS sales_bucket
FROM sales_table
'''
sales_table_execution_plan = sales_table.select(
    col("seller_id"),
    when(col("num_pieces_sold") < 30, "Lower than 30").
    when(col("num_pieces_sold") < 60, "Between 31 and 60").
    when(col("num_pieces_sold") < 90, "Between 61 and 90").
    otherwise("More than 91").alias("sales_bucket")
)

sales_table_execution_plan.show()

# Code Snippet 7
# Union All

'''
CREATE TABLE part_1 AS
SELECT *
FROM sales_table
WHERE num_pieces_sold > 50;
CREATE TABLE part_2 AS
SELECT *
FROM sales_table
WHERE num_pieces_sold <= 50;
SELECT *
FROM part_1
 UNION ALL
SELECT *
FROM part_2
'''
#   Split part 1
sales_table_execution_plan_part_1 = sales_table.where(col("num_pieces_sold") > 50)

#   Split part 2
sales_table_execution_plan_part_2 = sales_table.where(col("num_pieces_sold") <= 50)

#   Union back
sales_table_execution_plan = sales_table_execution_plan_part_1.unionByName(sales_table_execution_plan_part_2)

sales_table_execution_plan.explain()