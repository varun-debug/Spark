# to start pyspark
pyspark 

# to get into the rdd or read the csv 
df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")
