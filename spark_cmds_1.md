df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")
