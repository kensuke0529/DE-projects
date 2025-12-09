import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)

# ============================================================
# PARAMETERS
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

input_path = args["input_path"]
output_path = args["output_path"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Starting Silver ETL")
print(f"Input Path: {input_path}")
print(f"Output Path: {output_path}")

# ============================================================
# SCHEMA
event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField(
            "customer",
            StructType(
                [
                    StructField("customer_id", IntegerType()),
                    StructField("first_name", StringType()),
                    StructField("last_name", StringType()),
                    StructField("email", StringType()),
                    StructField("country", StringType()),
                    StructField("created_at", StringType()),
                    StructField("tax_rate", DoubleType()),
                ]
            ),
        ),
        StructField(
            "session",
            StructType(
                [
                    StructField("channel", StringType()),
                    StructField("device", StringType()),
                ]
            ),
        ),
        StructField(
            "product",
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                    StructField("category", StringType()),
                    StructField("price", DoubleType()),
                ]
            ),
        ),
        StructField(
            "cart",
            StructType(
                [
                    StructField("cart_id", StringType()),
                    StructField(
                        "items",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "product",
                                        StructType(
                                            [
                                                StructField("id", IntegerType()),
                                                StructField("name", StringType()),
                                                StructField("category", StringType()),
                                                StructField("price", DoubleType()),
                                            ]
                                        ),
                                    ),
                                    StructField("quantity", IntegerType()),
                                ]
                            )
                        ),
                    ),
                    StructField("created_at", StringType()),
                ]
            ),
        ),
        StructField(
            "order",
            StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", IntegerType()),
                    StructField(
                        "items",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "product",
                                        StructType(
                                            [
                                                StructField("id", IntegerType()),
                                                StructField("name", StringType()),
                                                StructField("category", StringType()),
                                                StructField("price", DoubleType()),
                                            ]
                                        ),
                                    ),
                                    StructField("quantity", IntegerType()),
                                ]
                            )
                        ),
                    ),
                    StructField("subtotal", DoubleType()),
                    StructField("tax", DoubleType()),
                    StructField("tax_rate", DoubleType()),
                    StructField("shipping", DoubleType()),
                    StructField("total_amount", DoubleType()),
                    StructField("currency", StringType()),
                    StructField("status", StringType()),
                    StructField("created_at", StringType()),
                    StructField("paid_at", StringType()),
                    StructField("fulfilled_at", StringType()),
                    StructField("cancelled_at", StringType()),
                    StructField("cancel_reason", StringType()),
                ]
            ),
        ),
        StructField("abandon_stage", StringType()),
    ]
)

# ============================================================
# READ BRONZE
df_bronze = spark.read.schema(event_schema).json(input_path)

print(f"Bronze count: {df_bronze.count()}")

# ============================================================
# TRANSFORM
df = df_bronze

df = df.withColumn(
    "event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

df = df.withColumn("event_date", F.to_date("event_timestamp"))
df = df.withColumn("processed_at", F.current_timestamp())

df = df.filter(F.col("event_id").isNotNull())

synthetic_item = F.struct(
    F.col("product").alias("product"), F.lit(1).cast(IntegerType()).alias("quantity")
)

df = df.withColumn(
    "items_array",
    F.when(F.col("order.items").isNotNull(), F.col("order.items"))
    .when(F.col("cart.items").isNotNull(), F.col("cart.items"))
    .when(F.col("product").isNotNull(), F.array(synthetic_item))
    .otherwise(F.array()),
)

# Explode items
df_ex = df.withColumn("item", F.explode_outer("items_array"))

df_silver = df_ex.select(
    "event_id",
    "event_type",
    "session_id",
    "event_timestamp",
    "event_date",
    "processed_at",
    F.col("customer.customer_id").alias("customer_id"),
    F.col("customer.first_name").alias("customer_first_name"),
    F.col("customer.last_name").alias("customer_last_name"),
    F.col("customer.email").alias("customer_email"),
    F.col("customer.country").alias("customer_country"),
    F.col("customer.tax_rate").alias("customer_tax_rate"),
    F.col("session.channel").alias("channel"),
    F.col("session.device").alias("device"),
    F.col("cart.cart_id").alias("cart_id"),
    "abandon_stage",
    F.col("order.order_id").alias("order_id"),
    F.col("order.subtotal").alias("order_subtotal"),
    F.col("order.tax").alias("order_tax"),
    F.col("order.tax_rate").alias("order_tax_rate"),
    F.col("order.shipping").alias("order_shipping"),
    F.col("order.total_amount").alias("order_total_amount"),
    F.col("order.status").alias("order_status"),
    F.col("order.created_at").alias("order_created_at_raw"),
    F.col("order.paid_at").alias("order_paid_at_raw"),
    F.col("order.fulfilled_at").alias("order_fulfilled_at_raw"),
    F.col("order.cancelled_at").alias("order_cancelled_at_raw"),
    F.col("order.cancel_reason").alias("order_cancel_reason"),
    F.col("item.product.id").alias("product_id"),
    F.col("item.product.name").alias("product_name"),
    F.col("item.product.category").alias("product_category"),
    F.col("item.product.price").alias("product_price"),
    F.col("item.quantity").alias("quantity"),
)

df_silver = df_silver.dropDuplicates(["event_id", "product_id", "order_id"])

print(f"Silver count: {df_silver.count()}")

# ============================================================
# WRITE SILVER
(
    df_silver.repartition("event_type", "event_date")
    .write.mode("append")
    .partitionBy("event_type", "event_date")
    .parquet(output_path)
)

print("Silver ETL SUCCESS.")
job.commit()
