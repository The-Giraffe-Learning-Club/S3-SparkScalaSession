import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Code Snippet 1
#   Initialize the Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("S3-02-Session") \
    .getOrCreate()

sales_table = spark.read.parquet("C:\\document++\\TheGiraffeLearningClub\\SparkScala\\data\\DatasetToCompleteTheSixSparkExercises\\sales_parquet")
sellers_table = spark.read.parquet("C:\\document++\\TheGiraffeLearningClub\\SparkScala\\data\\DatasetToCompleteTheSixSparkExercises\\sellers_parquet")

'''
SELECT a.*,
       b.*
FROM sales_table a
    LEFT JOIN sellers_table b
        ON a.seller_id = b.seller_id
'''
#   Left join
left_join_execution_plan = sales_table.join(sellers_table, 
                   on=sales_table["seller_id"] == sellers_table["seller_id"], 
                   how="left")

#   Inner join
inner_join_execution_plan = sales_table.join(sellers_table, 
                   on=sales_table["seller_id"] == sellers_table["seller_id"], 
                   how="inner")

#   Right join
right_join_execution_plan = sales_table.join(sellers_table, 
                   on=sales_table["seller_id"] == sellers_table["seller_id"], 
                   how="right")

#   Full Outer join
full_outer_join_execution_plan = sales_table.join(sellers_table, 
                   on=sales_table["seller_id"] == sellers_table["seller_id"], 
                   how="full_outer")

# Spark also supports semi and anti joins; these two are basically one way to express IN and NOT IN operations in Spark
# IN Clause
'''
SELECT *
FROM sales_table
WHERE seller_id IN (SELECT seller_id FROM sellers_table)
'''
#   Left Semi joins are a way to express the IN operation in SQL
semi_join_execution_plan = sales_table.join(sellers_table, 
                on=sales_table["seller_id"] == sellers_table["seller_id"], 
                how="left_semi")

semi_join_execution_plan.show()

# NOT IN Clause

'''
SELECT *
FROM sales_table
WHERE seller_id NOT IN (SELECT seller_id FROM sellers_table)
'''
#   Left Anti joins are a way to express the NOT IN operation in SQL
anti_join_execution_plan = sales_table.join(sellers_table,
                on=sales_table["seller_id"] == sellers_table["seller_id"],
                how="left_anti")

anti_join_execution_plan.show()


# Window Functions

'''
SELECT seller_id,
       product_id,
       total_pieces,
       dense_rank() OVER (PARTITION BY seller_id ORDER BY total_pieces DESC) as rank
FROM (
    SELECT seller_id,
           product_id,
           SUM(total_pieces_sold) AS total_pieces
    FROM sales_table
    GROUP BY seller_id,
           product_id
)
'''

sales_table_agg = sales_table.groupBy(col("seller_id"), col("product_id")).agg(sum("num_pieces_sold").alias("total_pieces"))

#   Define the Window: partition the table on the seller ID and sort 
#   each group according to the total pieces sold
window_specifications = Window.partitionBy(col("seller_id")).orderBy(col("total_pieces").asc())

#   Apply the dense_rank function, creating the window according to the specs above
sales_table_agg.withColumn('dense_rank', dense_rank().over(window_specifications)).show()


# Like Operation

'''
SELECT *
WHERE bill_raw_text LIKE 'ab%cd%'
'''
sales_table_execution_plan = sales_table.where(
    col('bill_raw_text').like("ab%cd%")
)


# Explode Function

'''
CREATE TABLE sales_table_aggregated AS
SELECT COLLECT_SET(num_pieces_sold) AS num_pieces_sold_set,
       seller_id
FROM sales_table
GROUP BY seller_id;
SELECT EXPLODE(num_pieces_sold_set) AS exploded_num_pieces_set
FROM sales_table_aggregated;
'''
sales_table_execution_aggregated = sales_table.groupBy(col("seller_id")).agg(
    collect_set(col("num_pieces_sold")).alias("num_pieces_sold_set")
)

sales_table_execution_exploded = sales_table_execution_aggregated.select(
    explode(col("num_pieces_sold_set")).alias("exploded_num_pieces_set")
)

sales_table_execution_exploded.show(10, True)

sales_table_execution_plan.show()