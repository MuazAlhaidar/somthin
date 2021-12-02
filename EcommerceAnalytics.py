from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Read CSV File into DataFrame").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_products_value", DoubleType(), True),
        StructField("order_freight_value", DoubleType(), True),
        StructField("order_items_qty", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("product_name_length", IntegerType(), True),
        StructField("product_description_length", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
    ]
)

"""
 1. id
 2. order_status
 3. order_products_value
 4. order_freight_value
 5. order_items_qty
 6. customer_city
 7. customer_state
 8. customer_zip_code_prefix
 9. product_name_length
10. product_description_length
11. product_photos_qty
12. review_score
13. order_purchase_timestamp
14. order_approved_at
15. order_delivered_customer_date
"""

data = spark.read.csv(
    "EdurekaSparkProjects/olist_public_dataset.csv", header=False, schema=schema
)


data.withColumn("order_purchase_date", to_date(col("order_purchase_timestamp"), "dd/mm/yy HH:mm"))
data.withColumn("order_approval_date", to_date(col("order_approved_at"), "dd/mm/yy HH:mm"))
data.withColumn("order_delivered_date", to_date(col("order_delivered_customer_date"), "dd/mm/yy HH:mm"))

# Daily Insights
data.withColumn("purchase_dayofyear", dayofyear(col("order_purchase_date")))
data.withColumn("approved_dayofyear", dayofyear(col("order_approval_date")))
data.withColumn("delivered_dayofyear", dayofyear(col("order_delivered_date")))

data.withColumn("approval_daily_time_taken", col("approved_dayofyear") - col("purchase_dayofyear"))
data.withColumn("delivery_daily_time_taken", col("delivered_dayofyear") - col("approved_dayofyear"))

# 1.a.1 Total Sales (order_products_value)
print("Daily Total Sales")
totalsales = (
    data.groupBy("purchase_dayofyear")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 1.a.2 Total Sales in each Customer City
print("Daily Total Sales in each City")
totalsales = (
    data.groupBy("purchase_dayofyear")
    .groupBy("customer_city")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 1.a.3 Total Sales in each Customer State
print("Daily Total Sales in each State")
totalsales = (
    data.groupBy("purchase_dayofyear")
    .groupBy("customer_state")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 1.b.1 Total number of orders sold
print("Daily Total Orders")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .agg(count("id").alias("orders_per_day"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.2 City wise order distribution
print("Daily Total Orders by City")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .groupBy("customer_city")
    .agg(count("id").alias("orders_per_day"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.3 State wise order distribution
print("Daily Total Orders by State")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .groupBy("customer_state")
    .agg(count("id").alias("orders_per_day"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.4 Average Review Score per Order
print("Daily Average Review Score")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .agg(avg("review_score").alias("average_score"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.5 Average Freight charges per Order
print("Daily Average Freight Charges")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .agg(avg("order_freight_value").alias("average_freight_charge"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.6 Average time taken to approve orders (Order Approved - Order Purchased)
print("Daily Average Approval Time")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .agg(avg("approval_daily_time_taken").alias("average_approval_time"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()

# 1.b.7 Average order delivery time
print("Daily Average Delivery Time")
totalorders = (
    data.groupBy("purchase_dayofyear")
    .agg(avg("delivery_daily_time_taken").alias("average_delivery_time"))
    .orderBy(asc("purchase_dayofyear"))
)
totalorders.show()


# Weekly Insights
data.withColumn("purchase_weekofyear", weekofyear(col("order_purchase_date")))
data.withColumn("approved_weekofyear", weekofyear(col("order_approval_date")))
data.withColumn("delivered_weekofyear", weekofyear(col("order_delivered_date")))

data.withColumn("approval_weekly_time_taken", col("approved_weekofyear") - col("purchase_weekofyear"))
data.withColumn("delivery_weekly_time_taken", col("delivered_weekofyear") - col("approved_weekofyear"))

# 2.a.1 Total Sales (order_products_value)
print("Weekly Total Sales")
totalsales = (
    data.groupBy("purchase_weekofyear")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 2.a.2 Total Sales in each Customer City
print("Weekly Total Sales in each City")
totalsales = (
    data.groupBy("purchase_weekofyear")
    .groupBy("customer_city")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 2.a.3 Total Sales in each Customer State
print("Weekly Total Sales in each State")
totalsales = (
    data.groupBy("purchase_weekofyear")
    .groupBy("customer_state")
    .agg(sum("order_products_value").alias("totalsales"))
    .orderBy(desc("totalsales"))
)
totalsales.show()

# 2.b.1 Total number of orders sold
print("Weekly Total Orders")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(count("id").alias("orders_per_week"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.2 City wise order distribution
print("Weekly Total Orders by City")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .groupBy("customer_city")
    .agg(count("id").alias("orders_per_week"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.3 State wise order distribution
print("Weekly Total Orders by State")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .groupBy("customer_state")
    .agg(count("id").alias("orders_per_week"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.4 Average Review Score per Order
print("Weekly Average Review Score")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(avg("review_score").alias("average_score"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.5 Average Freight charges per Order
print("Weekly Average Freight Charge")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(avg("order_freight_value").alias("average_freight_charge"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.6 Average time taken to approve orders (Order Approved - Order Purchased)
print("Weekly Average Approval Time")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(avg("approval_weekly_time_taken").alias("average_approval_time"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.b.7 Average order delivery time
print("Weekly Average Delivery Time")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(avg("delivery_weekly_time_taken").alias("average_delivery_time"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.c.1 Total Freight Charges
print("Weekly Total Freight Charge")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .agg(sum("order_freight_value").alias("total_freight_charge"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()

# 2.d.1 Freight charges distribution in each customer city
print("Weekly Total Freight Charge in each City")
totalorders = (
    data.groupBy("purchase_weekofyear")
    .groupBy("customer_city")
    .agg(sum("order_freight_value").alias("total_freight_charge"))
    .orderBy(asc("purchase_weekofyear"))
)
totalorders.show()