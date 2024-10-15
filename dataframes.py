from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, current_timestamp

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