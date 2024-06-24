CREATE TABLE Employee_Details
(
Id INT,
First_Name VARCHAR(20),
Last_Name VARCHAR(20),
Age INT,
City VARCHAR(20),
Product VARCHAR(20),
Joining_date VARCHAR(30)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'sparkproject2/Employee_Details.csv' INTO TABLE Employee_Details;

Location
hdfs://ip-172-31-32-214.ap-south-1.compute.internal:8020/user/hive/warehouse/employee.db/employee_details

---------------------------------------------------------
CREATE TABLE Employee_Number
(
Id INT,
Mobile_num INT,
Country VARCHAR(5)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'spark_project/Employee_number.csv' INTO TABLE Employee_Number;

Location
hdfs://ip-172-31-32-214.ap-south-1.compute.internal:8020/user/hive/warehouse/employee.db/employee_number

-----------------------------------------------------
CREATE TABLE Employee_Sales
(
Product VARCHAR(20),
Sales INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'spark_project/Employee_sales.csv' INTO TABLE Employee_Sales;

Location
hdfs://ip-172-31-32-214.ap-south-1.compute.internal:8020/user/hive/warehouse/employee.db/employee_sales
