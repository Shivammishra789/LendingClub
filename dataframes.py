from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, current_timestamp,regexp_replace, col

spark = SparkSession.builder.master("local[1]") \
        .appName("SparkPractice") \
        .getOrCreate()

customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'


customers_raw_data = spark.read \
        .format("csv") \
        .schema(customer_schema) \
        .option("header","true") \
        .load("C:/Users/Vaishnavi/pyspark/accepted_2007_to_2018Q4.csv")

# customers_raw_data1 = customers_raw_data.withColumn("emp_id",sha2(concat_ws('||',*['emp_title','emp_length','home_ownership','annual_inc','zip_code','addr_state','grade','sub_grade','verification_status']),256))


customers_raw_data_new_df = customers_raw_data.withColumnRenamed("annual_inc", "annual_income") \
                                                .withColumnRenamed("addr_state", "address_state") \
                                                .withColumnRenamed("zip_code", "address_zipcode") \
                                                .withColumnRenamed("country", "address_country") \
                                                .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
                                                .withColumnRenamed("annual_inc_joint", "join_annual_income")


cus_new_df = customers_raw_data_new_df.withColumn("ingest_date",current_timestamp())

cus_new_df.count()
print('distinct customers')
customers_distinct = cus_new_df.distinct()

customers_distinct.createOrReplaceTempView("customers")
spark.sql("select * from customers")
spark.sql("select count(*) from customers where annual_income is null")
customers_income_filtered = spark.sql("select * from customers where annual_income is not null")
customers_income_filtered.createOrReplaceTempView("customers")
spark.sql("select count(*) from customers where annual_income is null")
spark.sql("select distinct(emp_length) from customers")

customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)",""))

customers_emplength_cleaned.printSchema()

customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))
customers_emplength_casted.printSchema()

customers_emplength_casted.filter("emp_length is null").count()
customers_emplength_casted.createOrReplaceTempView("customers")
avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()

print(avg_emp_length)
avg_emp_duration = avg_emp_length[0][0]
print(avg_emp_duration)
customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])

customers_emplength_replaced.filter("emp_length is null").count()
customers_emplength_replaced.createOrReplaceTempView("customers")
spark.sql("select distinct(address_state) from customers")

spark.sql("select count(address_state) from customers where length(address_state)>2")

from pyspark.sql.functions import when, col, length

customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state"))
)

customers_state_cleaned.select("address_state").distinct()

customers_state_cleaned.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/customers_parquet") \
    .save()

customers_state_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/customers_csv") \
    .save()

