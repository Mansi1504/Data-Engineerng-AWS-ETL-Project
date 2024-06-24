#! /usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace 
from pyspark.sql.functions import col, to_date, current_date, datediff, floor, lit
from pyspark.sql.functions import substring

spark = SparkSession.builder \
    .appName("HiveIntegrationExample") \
    .enableHiveSupport() \
    .getOrCreate()


###importing tables from hive warehouse
df1 = spark.sql("""SELECT * FROM Employee_2.Employee_Details""")
df1.show()

df2 = spark.sql("""SELECT * FROM Employee_2.Employee_number""")
df2.show()

df3 = spark.sql("""SELECT * FROM Employee_2.Employee_Sales""")
df3.show()

#to remove special characters from a column
pattern = "[^a-zA-Z0-9 ]"
df1_new = df1.withColumn("New_Name", regexp_replace(df1["First_Name"], pattern, ""))
df1_new.show()

#to drop a column from a table
df1_new = df1_new.drop("First_Name")
df1_new.show()

#concatinating
df1_new = df1_new.withColumn("Full_Name", concat(df1_new.New_Name,lit(" "),df1_new.Last_Name))

#finding total experience of an employee
df1_new_exp = df1_new.withColumn("Year", substring(col("joining_date"), -2, 2))
df1_new_exp.show()

df1_new_exp = df1_new_exp.withColumn("Experience", 24 - col("Year"))
df.show()

#Creating a temp view to start writing in sql
Exp.createOrReplaceTempView("New_Employee")

#Converting double data type in int
Exp = spark.sql("""
    SELECT *, CAST(Experience AS INT) AS Exp
    FROM New_Employee
""")

#Calculating appraisal

spark.sql("""SELECT 
Id, Age, City, Product, joining_date, Full_Name, year,Exp, 
CASE 
WHEN Exp <= 2 THEN "10%" 
WHEN Exp >2 AND Exp <=3 THEN "20%" 
ELSE "30%"
END AS Appraisal 
FROM New_Employee""").show()

#Concatinating Country Code
df2 = df2.withColumn("New_Number", concat(lit("+91-"),df2.mobile_num))

df_output = spark.sql("""SELECT d.id, d.name,m.mobile_num FROM Employee.Employee_Details d LEFT JOIN Employee.Employee_number m ON d.id = m.id WHERE m.id IS NULL""")
df_output.show()

###Writing table in hive from pyspark
df_output.write.mode("overwrite").saveAsTable("Employee.Employee_Result")

#to be written in linux to run pyspark script
spark-submit --master yarn --deploy-mode client example.py
