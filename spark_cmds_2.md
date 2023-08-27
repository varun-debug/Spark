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
 
 
