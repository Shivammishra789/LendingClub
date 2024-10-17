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

#####################################################################################################

loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'

loans_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loans_schema) \
.load("C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/loans_data_csv")

loans_raw_df.printSchema()

from pyspark.sql.functions import current_timestamp

loans_df_ingestd = loans_raw_df.withColumn("ingest_date", current_timestamp())

loans_df_ingestd.createOrReplaceTempView("loans")
spark.sql("select count(*) from loans")
spark.sql("select * from loans where loan_amount is null")

columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]

loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)

loans_filtered_df.count()

loans_filtered_df.createOrReplaceTempView("loans")

from pyspark.sql.functions import regexp_replace, col

loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
.cast("int") / 12) \
.cast("int")) \
.withColumnRenamed("loan_term_months","loan_term_years")

loans_term_modified_df.printSchema()

loans_term_modified_df.createOrReplaceTempView("loans")

spark.sql("select distinct(loan_purpose) from loans")
spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]

from pyspark.sql.functions import when

loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))
loans_purpose_modified.createOrReplaceTempView("loans")
spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

from pyspark.sql.functions import count

loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())

loans_purpose_modified.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/loans_parquet") \
    .save()

loans_purpose_modified.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/loans_csv") \
    .save()

##############################lendingproject3###########################################

loans_repay_raw_df = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferSchema", True) \
    .load("C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/loans_repayments_csv")

loans_repay_raw_df.printSchema()

loans_repay_schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'

loans_repay_raw_df = spark.read \
    .format("csv") \
    .option("header",True) \
    .schema(loans_repay_schema) \
    .load("/public/trendytech/lendingclubproject/raw/loans_repayments_csv")

loans_repay_raw_df.printSchema()

from pyspark.sql.functions import current_timestamp

loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date", current_timestamp())

loans_repay_df_ingestd.printSchema()

loans_repay_df_ingestd.count()

loans_repay_df_ingestd.createOrReplaceTempView("loan_repayments")

spark.sql("select count(*) from loan_repayments where total_principal_received is null")

columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]

loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)

loans_repay_filtered_df.count()

loans_repay_filtered_df.createOrReplaceTempView("loan_repayments")

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0")

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0")

spark.sql("select * from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0")

from pyspark.sql.functions import when, col


loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
   "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )

loans_payments_fixed_df.filter("loan_id == '1064185'")

loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")

loans_payments_fixed2_df.filter("last_payment_date = 0.0").count()

loans_payments_fixed2_df.filter("next_payment_date = 0.0").count()

loans_payments_fixed2_df.filter("last_payment_date is null").count()

loans_payments_fixed2_df.filter("next_payment_date is null").count()

loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
  "last_payment_date",
   when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
    )


loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
  "last_payment_date",
   when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
    )

loans_payments_ndate_fixed_df.filter("last_payment_date = 0.0").count()

loans_payments_ndate_fixed_df.filter("next_payment_date = 0.0").count()

loans_payments_ndate_fixed_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/loans_repayments_parquet") \
    .save()


loans_payments_ndate_fixed_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "C:/Users/Vaishnavi/pyspark/lendingclubproject/raw/cleaned/loans_repayments_csv") \
    .save()

