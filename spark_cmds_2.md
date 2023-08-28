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

# drop the duplicate vales
empdf.dropDuplicates().show()

# drop the duplicates according to the column wise 
empdf.dropDuplicates(["DEPARTMENT_ID","HIRE_DATE"]).select("EMPLOYEE_ID","DEPARTMENT_ID","HIRE_DATE").show()

# use the sql functions 
from pyspark.sql.functions import *

empdf.count()

empdf.select(count("*")).show()
+--------+
|count(1)|
+--------+
|      50|
+--------+

#adding allias to it
empdf.select(count("*").alias("Total_Count")).show()
+-----------+
|Total_Count|
+-----------+
|         50|
+-----------+

# max and min 
empdf.select(max("SALARY").alias("Max_Sal")).show()
+-------+
|Max_Sal|
+-------+
|  24000|
+-------+

>>> empdf.select(min("SALARY").alias("MIN_Sal")).show()
+-------+
|MIN_Sal|
+-------+
|   2100|
+-------+
# avg and sum
 empdf.select(avg("SALARY").alias("Avg_Sal")).show()
+-------+
|Avg_Sal|
+-------+
|6182.32|
+-------+

>>> empdf.select(sum("SALARY").alias("Total_sum")).show()
+---------+
|Total_sum|
+---------+
|   309116|
+---------+

# orderBy in asc
empdf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","Salary").orderBy("SALARY").show()

# orderBY in desc
empdf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","Salary").orderBy(col("SALARY").desc()).show()

# multi column ordering 
empdf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","Salary").orderBy(col("DEPARTMENT_ID").asc(),col("SALARY").desc()).show()

# grouping 
empdf.groupBy("DEPARTMENT_ID").sum("Salary").show()
 same goes with min and max

# grouping with multiple columns
empdf.groupBy("DEPARTMENT_ID","Job_ID").sum("Salary").show()

empdf.groupBy("DEPARTMENT_ID","Job_ID").sum("Salary","EMPLOYEE_ID").show()

# preffered way of grouping as we can give alias
empdf.groupBy("DEPARTMENT_ID").agg( sum("Salary").alias("Sum_of_Sal")).show() 

empdf.groupBy("DEPARTMENT_ID").agg( sum("Salary").alias("Sum_of_Sal"), max("Salary").alias("MAx_Sal"), min("Salary").alias("Min_Sal")).show()
+-------------+----------+-------+-------+
|DEPARTMENT_ID|Sum_of_Sal|MAx_Sal|Min_Sal|
+-------------+----------+-------+-------+
|           20|     19000|  13000|   6000|
|           40|      6500|   6500|   6500|
|          100|     51608|  12008|   6900|
|           10|      4400|   4400|   4400|
|           50|     85600|   8200|   2100|
|           70|     10000|  10000|  10000|
|           90|     58000|  24000|  17000|
|           60|     28800|   9000|   4200|
|          110|     20308|  12008|   8300|
|           30|     24900|  11000|   2500|
+-------------+----------+-------+-------+

# if-else statements
empdf.withColumn("Emp_Grade", when(col("Salary") > 15000, "A").when((col("Salary") >= 10000) & (col("Salary") < 10000),"B").otherwise("C")).select("EMPLOYEE_ID","FIRST_NAME","Emp_Grade").show()
+-----------+----------+---------+
|EMPLOYEE_ID|FIRST_NAME|Emp_Grade|
+-----------+----------+---------+
|        198|    Donald|        C|
|        199|   Douglas|        C|
|        200|  Jennifer|        C|
|        201|   Michael|        C|
|        202|       Pat|        C|
|        203|     Susan|        C|
|        204|   Hermann|        C|
|        205|   Shelley|        C|
|        206|   William|        C|
|        100|    Steven|        A|
|        101|     Neena|        A|
|        102|       Lex|        A|
|        103| Alexander|        C|
|        104|     Bruce|        C|
|        105|     David|        C|
|        106|     Valli|        C|
|        107|     Diana|        C|
|        108|     Nancy|        C|
|        109|    Daniel|        C|
|        110|      John|        C|
+-----------+----------+---------+
only showing top 20 rows


