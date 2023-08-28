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

we can use "\" to use the long code in multiple lines

# how to use as sql creating temporary view 
This allows you to query the DataFrame using SQL-like syntax using the spark.sql() method. Let's break down the usage of 

empdf.CreateOrReplaceTempView("employee")

df1 = spark.sql("Select * from employee")
>>> df1.show(5)
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|  MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|  MK_REP|  6000|            - |       201|           20|
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+
>>> df2 = spark.sql("Select department_id,sum(Salary) as Total_Salary from employee group by department_id")
>>> df2.show()
+-------------+------------+
|department_id|Total_Salary|
+-------------+------------+
|           20|       19000|
|           40|        6500|
|          100|       51608|
|           10|        4400|
|           50|       85600|
|           70|       10000|
|           90|       58000|
|           60|       28800|
|          110|       20308|
|           30|       24900|
+-------------+------------+

# joins
deptdf = spark.read.option("header",True).option("Inferschema",True).csv("/input_data/departments.csv")

empdf.join(deptdf,empdf.DEPARTMENT_ID == deptdf.DEPARTMENT_ID,"inner").show() // just change the inner with full/fullouter,left and right for other type of joins

# self join
empdf.alias("emp1").join(empdf.alias("emp2"),col("emp1.MANAGER_ID") == col("emp2.EMPLOYEE_ID"),"inner")\
... .select(col("emp1.MANAGER_ID"),col("emp2.FIRST_NAME"),col("emp2.LAST_NAME")).dropDuplicates().show()
+----------+----------+---------+
|MANAGER_ID|FIRST_NAME|LAST_NAME|
+----------+----------+---------+
|       122|     Payam| Kaufling|
|       101|     Neena|  Kochhar|
|       100|    Steven|     King|
|       205|   Shelley|  Higgins|
|       114|       Den| Raphaely|
|       103| Alexander|   Hunold|
|       124|     Kevin|  Mourgos|
|       120|   Matthew|    Weiss|
|       123|    Shanta|  Vollman|
|       121|      Adam|    Fripp|
|       108|     Nancy|Greenberg|
|       102|       Lex|  De Haan|
|       201|   Michael|Hartstein|
+----------+----------+---------+

# multiple table join
create one more table just for join purpose

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
>>> location_data = [(1700,"India"),(1800,"USA")]
>>> schema = StructType([StructField("Location_Id",IntegerType(),True),StructField("Location_Name",StringType(),True)])
>>> locdf = spark.createDataFrame(data = location_data, schema = schema)
>>> locdf.show()
+-----------+-------------+
|Location_Id|Location_Name|
+-----------+-------------+
|       1700|        India|
|       1800|          USA|
+-----------+-------------+
>>> empdf.join(deptdf,(empdf.DEPARTMENT_ID == deptdf.DEPARTMENT_ID) & (deptdf.LOCATION_ID == 1700),"inner").join(locdf, deptdf.LOCATION_ID == locdf.Location_Id,"inner").select(empdf.EMPLOYEE_ID,empdf.DEPARTMENT_ID,deptdf.DEPARTMENT_NAME,locdf.Location_Name).show()
+-----------+-------------+---------------+-------------+
|EMPLOYEE_ID|DEPARTMENT_ID|DEPARTMENT_NAME|Location_Name|
+-----------+-------------+---------------+-------------+
|        205|          110|     Accounting|        India|
|        206|          110|     Accounting|        India|
|        108|          100|        Finance|        India|
|        109|          100|        Finance|        India|
|        110|          100|        Finance|        India|
|        111|          100|        Finance|        India|
|        112|          100|        Finance|        India|
|        113|          100|        Finance|        India|
|        100|           90|      Executive|        India|
|        101|           90|      Executive|        India|
|        102|           90|      Executive|        India|
|        114|           30|     Purchasing|        India|
|        115|           30|     Purchasing|        India|
|        116|           30|     Purchasing|        India|
|        117|           30|     Purchasing|        India|
|        118|           30|     Purchasing|        India|
|        119|           30|     Purchasing|        India|
|        200|           10| Administration|        India|
+-----------+-------------+---------------+-------------+

# User defined functions in spark
first create using @udf

@udf(returnType=StringType())
... def UpperCase(in_str):
...     out_str = in_str.upper()
...     return out_str

then call that function like other functions
 empdf.select(col("EMPLOYEE_ID"),col("FIRST_NAME"),col("LAST_NAME"),UpperCase(col("FIRST_NAME")).alias("Upper_Case_First"),UpperCase(col("LAST_NAME")).alias("Upper_Case_Last")).show()
+-----------+----------+---------+----------------+---------------+             
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|Upper_Case_First|Upper_Case_Last|
+-----------+----------+---------+----------------+---------------+
|        198|    Donald| OConnell|          DONALD|       OCONNELL|
|        199|   Douglas|    Grant|         DOUGLAS|          GRANT|
|        200|  Jennifer|   Whalen|        JENNIFER|         WHALEN|
|        201|   Michael|Hartstein|         MICHAEL|      HARTSTEIN|
|        202|       Pat|      Fay|             PAT|            FAY|
|        203|     Susan|   Mavris|           SUSAN|         MAVRIS|
|        204|   Hermann|     Baer|         HERMANN|           BAER|
|        205|   Shelley|  Higgins|         SHELLEY|        HIGGINS|
|        206|   William|    Gietz|         WILLIAM|          GIETZ|
|        100|    Steven|     King|          STEVEN|           KING|
|        101|     Neena|  Kochhar|           NEENA|        KOCHHAR|
|        102|       Lex|  De Haan|             LEX|        DE HAAN|
|        103| Alexander|   Hunold|       ALEXANDER|         HUNOLD|
|        104|     Bruce|    Ernst|           BRUCE|          ERNST|
|        105|     David|   Austin|           DAVID|         AUSTIN|
|        106|     Valli|Pataballa|           VALLI|      PATABALLA|
|        107|     Diana|  Lorentz|           DIANA|        LORENTZ|
|        108|     Nancy|Greenberg|           NANCY|      GREENBERG|
|        109|    Daniel|   Faviet|          DANIEL|         FAVIET|
|        110|      John|     Chen|            JOHN|           CHEN|
+-----------+----------+---------+----------------+---------------+
only showing top 20 rows

# windows function
from pyspark.sql.window import Window
>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy("Salary")
>>> empdf.withColumn("salary_rank",rank().over(windowSpec)).select("DEPARTMENT_ID","SALARY","salary_rank").show()
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|salary_rank|
+-------------+------+-----------+
|           10|  4400|          1|
|           20|  6000|          1|
|           20| 13000|          2|
|           30|  2500|          1|
|           30|  2600|          2|
|           30|  2800|          3|
|           30|  2900|          4|
|           30|  3100|          5|
|           30| 11000|          6|
|           40|  6500|          1|
|           50|  2100|          1|
|           50|  2200|          2|
|           50|  2200|          2|
|           50|  2400|          4|
|           50|  2400|          4|
|           50|  2500|          6|
|           50|  2500|          6|
|           50|  2600|          8|
|           50|  2600|          8|
|           50|  2700|         10|
+-------------+------+-----------+
only showing top 20 rows


Now useing Dense_rank
empdf.withColumn("salary_rank",dense_rank().over(windowSpec)).select("DEPARTMENT_ID","SALARY","salary_rank").show()
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|salary_rank|
+-------------+------+-----------+
|           10|  4400|          1|
|           20|  6000|          1|
|           20| 13000|          2|
|           30|  2500|          1|
|           30|  2600|          2|
|           30|  2800|          3|
|           30|  2900|          4|
|           30|  3100|          5|
|           30| 11000|          6|
|           40|  6500|          1|
|           50|  2100|          1|
|           50|  2200|          2|
|           50|  2200|          2|
|           50|  2400|          3|
|           50|  2400|          3|
|           50|  2500|          4|
|           50|  2500|          4|
|           50|  2600|          5|
|           50|  2600|          5|
|           50|  2700|          6|
+-------------+------+-----------+
only showing top 20 rows


# sum means rollinng sum department wise
empdf.withColumn("salary_running_sum", sum("Salary").over(windowSpec)).select("DEPARTMENT_ID","SALARY","salary_running_sum").show()

# save the results
result_df_new = empdf.join(deptdf,empdf.DEPARTMENT_ID == deptdf.DEPARTMENT_ID,"inner").drop(deptdf.MANAGER_ID).drop(deptdf.DEPARTMENT_ID)

result_df_new.write.mode("overwrite").option("header",True).save("/output/result")

 hdfs dfs -ls /output/result
Found 2 items
-rw-r--r--   1 abc supergroup          0 2023-08-28 18:47 /output/result/_SUCCESS
-rw-r--r--   1 abc supergroup       6609 2023-08-28 18:47 /output/result/part-00000-ed7ec1b4-7b7e-43d4-b9be-2810b85e021b-c000.snappy.parquet


#for csv format save
result_df_new.write.mode("overwrite").option("header",True).format("csv").save("/output/result")

abc@8ae3505765c7:~/workspace$ hdfs dfs -ls /output/result
Found 2 items
-rw-r--r--   1 abc supergroup          0 2023-08-28 18:50 /output/result/_SUCCESS
-rw-r--r--   1 abc supergroup       4410 2023-08-28 18:50 /output/result/part-00000-c32d421a-6729-4b8e-9fdb-1558b54ba566-c000.csv

# partition by department name
#we can use append also instead of override if we do not want to overwrite the file
result_df_new.write.mode("overwrite").partitionBy("DEPARTMENT_NAME").option("header",True).format("csv").save("/output/result")

#here is how partition will look after saving
abc@8ae3505765c7:~/workspace$ hdfs dfs -ls /output/result
Found 11 items
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Accounting
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Administration
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Executive
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Finance
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Human Resources
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=IT
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Marketing
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Public Relations
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Purchasing
drwxr-xr-x   - abc supergroup          0 2023-08-28 18:52 /output/result/DEPARTMENT_NAME=Shipping
-rw-r--r--   1 abc supergroup          0 2023-08-28 18:52 /output/result/_SUCCESS
