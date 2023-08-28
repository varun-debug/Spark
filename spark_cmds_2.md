# read the csv file emp 
empdf = spark.read.option("header",True).option("InferSchema",True).csv("/input_data/employees.csv")

# select to show only specific columns
empdf.select("EMPLOYEE_ID","FIRST_NAME","LAST_NAME").show(5)
+-----------+----------+---------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|
+-----------+----------+---------+
|        198|    Donald| OConnell|
|        199|   Douglas|    Grant|
|        200|  Jennifer|   Whalen|
|        201|   Michael|Hartstein|
|        202|       Pat|      Fay|
+-----------+----------+---------+

# create the new df or rdd of specific col in it
df = empdf.select("EMPLOYEE_ID","FIRST_NAME","LAST_NAME")
df.show(5)
+-----------+----------+---------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|
+-----------+----------+---------+
|        198|    Donald| OConnell|
|        199|   Douglas|    Grant|
|        200|  Jennifer|   Whalen|
|        201|   Michael|Hartstein|
|        202|       Pat|      Fay|
+-----------+----------+---------+
only showing top 5 rows

# another way to call the col without inverted commans i.e. Second systax
empdf.select(empdf.EMPLOYEE_ID,empdf.FIRST_NAME,empdf.LAST_NAME).show(5)

# third syntax
empdf.select(empdf["EMPLOYEE_ID"],empdf["FIRST_NAME"],empdf["LAST_NAME"]).show(5)

 # Fourth syntax(preffered way)
from pyspark.sql.functions import col
empdf.select(col("EMPLOYEE_ID"),col("FIRST_NAME"),col("LAST_NAME")).show(5)

# to use alias names

empdf.select(col("EMPLOYEE_ID").alias("Emp_ID"),col("FIRST_NAME").alias("F_Name"),col("LAST_NAME").alias("L_Name")).show(5)
+------+--------+---------+
|Emp_ID|  F_Name|   L_Name|
+------+--------+---------+
|   198|  Donald| OConnell|
|   199| Douglas|    Grant|
|   200|Jennifer|   Whalen|
|   201| Michael|Hartstein|
|   202|     Pat|      Fay|
+------+--------+---------+
only showing top 5 rows

# showing new column created from new calculations
empdf.select("EMPLOYEE_ID","FIRST_NAME","SALARY").withColumn("New_Salary",col("Salary") + 1000).show()

EMPLOYEE_ID|FIRST_NAME|SALARY|New_Salary|
+-----------+----------+------+----------+
|        198|    Donald|  2600|      3600|
|        199|   Douglas|  2600|      3600|
|        200|  Jennifer|  4400|      5400|
|        201|   Michael| 13000|     14000|
|        202|       Pat|  6000|      7000|
|        203|     Susan|  6500|      7500|
|        204|   Hermann| 10000|     11000|
|        205|   Shelley| 12008|     13008|
|        206|   William|  8300|      9300|
|        100|    Steven| 24000|     25000|
|        101|     Neena| 17000|     18000|
|        102|       Lex| 17000|     18000|
|        103| Alexander|  9000|     10000|
|        104|     Bruce|  6000|      7000|
|        105|     David|  4800|      5800|
|        106|     Valli|  4800|      5800|
|        107|     Diana|  4200|      5200|
|        108|     Nancy| 12008|     13008|
|        109|    Daniel|  9000|     10000|
|        110|      John|  8200|      9200|
 
 # how to get chanining means if we do not want to put old salary col but only the new updated salary col for above scenario
 
empdf.withColumn("Salary",col("Salary") + 1000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show()

+-----------+----------+------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|
+-----------+----------+------+
|        198|    Donald|  3600|
|        199|   Douglas|  3600|
|        200|  Jennifer|  5400|
|        201|   Michael| 14000|
|        202|       Pat|  7000|
|        203|     Susan|  7500|
|        204|   Hermann| 11000|
|        205|   Shelley| 13008|
|        206|   William|  9300|
|        100|    Steven| 25000|
..............................
