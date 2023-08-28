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

# rename the column name
empdf.withColumnRenamed("SALARY","EMP_SALARY").show()

# delete or drop the column
 empdf.drop("COMMISSION_PCT").show()

# Apply filters
empdf.filter(col("SALARY")> 5000).select("FIRST_NAME","SALARY").show()

# using and applying multiple filters

empdf.filter((col("SALARY")> 5000) & (col("DEPARTMENT_ID")== 50)).select("DEPARTMENT_ID","FIRST_NAME","SALARY").show()
+-------------+----------+------+
|DEPARTMENT_ID|FIRST_NAME|SALARY|
+-------------+----------+------+
|           50|   Matthew|  8000|
|           50|      Adam|  8200|
|           50|     Payam|  7900|
|           50|    Shanta|  6500|
|           50|     Kevin|  5800|
+-------------+----------+------+

# another simple way
empdf.filter(" SALARY > 5000 and  DEPARTMENT_ID != 50").show(5)

empdf.filter(" SALARY > 5000 and  DEPARTMENT_ID == 50").show(5)
+-----------+----------+---------+--------+------------+---------+------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+------+------+--------------+----------+-------------+
|        120|   Matthew|    Weiss|  MWEISS|650.123.1234|18-JUL-04|ST_MAN|  8000|            - |       100|           50|
|        121|      Adam|    Fripp|  AFRIPP|650.123.2234|10-APR-05|ST_MAN|  8200|            - |       100|           50|
|        122|     Payam| Kaufling|PKAUFLIN|650.123.3234|01-MAY-03|ST_MAN|  7900|            - |       100|           50|
|        123|    Shanta|  Vollman|SVOLLMAN|650.123.4234|10-OCT-05|ST_MAN|  6500|            - |       100|           50|
|        124|     Kevin|  Mourgos|KMOURGOS|650.123.5234|16-NOV-07|ST_MAN|  5800|            - |       100|           50|
+-----------+----------+---------+--------+------------+---------+------+------+--------------+----------+-------------+

empdf.filter(" DEPARTMENT_ID != 50").show(5)

