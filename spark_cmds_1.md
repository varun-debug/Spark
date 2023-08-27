# to start pyspark
pyspark 

# to get into the rdd or read the csv 
df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")

# to print the schema rdd
df2.printSchema()

# to print the data of the rdd
df2.show()

# to show specific amount of rows i.e. first 5 rows
df2.show(5)
