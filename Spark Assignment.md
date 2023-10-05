```python
!pip install pyspark
```

    Requirement already satisfied: pyspark in c:\users\varukish\appdata\local\anaconda3\lib\site-packages (3.5.0)
    Requirement already satisfied: py4j==0.10.9.7 in c:\users\varukish\appdata\local\anaconda3\lib\site-packages (from pyspark) (0.10.9.7)
    


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
```


```python
spark = SparkSession.builder.appName("MySparkApp").config("spark.some.config.option", "config-value").getOrCreate()

```


```python
print("Spark version:", spark.version)
```

    Spark version: 3.5.0
    


```python
case = "C:/Users/VARUKISH/OneDrive - Capgemini/Desktop/Certificate/Data Engineer/Spark/archive/Case.csv"
region = "C:/Users/VARUKISH/OneDrive - Capgemini/Desktop/Certificate/Data Engineer/Spark/archive/Region.csv"
time = "C:/Users/VARUKISH/OneDrive - Capgemini/Desktop/Certificate/Data Engineer/Spark/archive/TimeProvince.csv"
```


```python
#creating dataframes
df_case = spark.read.option("header",True).option("inferschema",True).csv(case)
df_region = spark.read.option("header",True).option("inferschema",True).csv(region)
df_time = spark.read.option("header",True).option("inferschema",True).csv(time)
df_case.show(5)
df_region.show(5)
df_time.show(5)
```

    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | case_id|province|        city|group|      infection_case|confirmed| latitude| longitude|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | 1000001|   Seoul|  Yongsan-gu| true|       Itaewon Clubs|      139|37.538621|126.992652|
    | 1000002|   Seoul|   Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|
    | 1000003|   Seoul|     Guro-gu| true| Guro-gu Call Center|       95|37.508163|126.884387|
    | 1000004|   Seoul|Yangcheon-gu| true|Yangcheon Table T...|       43|37.546061|126.874209|
    | 1000005|   Seoul|   Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    only showing top 5 rows
    
    +-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    | code|province|       city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
    +-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |10000|   Seoul|      Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |10010|   Seoul| Gangnam-gu|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
    |10020|   Seoul|Gangdong-gu|37.530492|127.123837|                     27|                32|               0|         1.54|                   14.55|                5.4|              1023|
    |10030|   Seoul| Gangbuk-gu|37.639938|127.025508|                     14|                21|               0|         0.67|                   19.49|                8.5|               628|
    |10040|   Seoul| Gangseo-gu|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|
    +-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    only showing top 5 rows
    
    +----------+----+--------+---------+--------+--------+
    |      date|time|province|confirmed|released|deceased|
    +----------+----+--------+---------+--------+--------+
    |2020-01-20|  16|   Seoul|        0|       0|       0|
    |2020-01-20|  16|   Busan|        0|       0|       0|
    |2020-01-20|  16|   Daegu|        0|       0|       0|
    |2020-01-20|  16| Incheon|        1|       0|       0|
    |2020-01-20|  16| Gwangju|        0|       0|       0|
    +----------+----+--------+---------+--------+--------+
    only showing top 5 rows
    
    


```python
# count of records od each dataframe
print("Case Dataset row count:",df_case.count())
print("Region Dataset row count:",df_region.count())
print("TimeProvince Dataset row count:",df_time.count())
```

    Case Dataset row count: 174
    Region Dataset row count: 244
    TimeProvince Dataset row count: 2771
    


```python
# drop the duplicate vales
df_case.dropDuplicates()
df_region.dropDuplicates()
df_time.dropDuplicates()
```




    DataFrame[date: date, time: int, province: string, confirmed: int, released: int, deceased: int]




```python
# describing dataframe 
df_case.describe()
```




    DataFrame[summary: string,  case_id: string, province: string, city: string, infection_case: string, confirmed: string, latitude: string, longitude: string]




```python
# describing dataframe 
df_region.describe()
```




    DataFrame[summary: string, code: string, province: string, city: string, latitude: string, longitude: string, elementary_school_count: string, kindergarten_count: string, university_count: string, academy_ratio: string, elderly_population_ratio: string, elderly_alone_ratio: string, nursing_home_count: string]




```python
# describing dataframe 
df_time.describe()
```




    DataFrame[summary: string, time: string, province: string, confirmed: string, released: string, deceased: string]




```python
# use of limit function
df_case_limit = df_case.limit(5)
df_case_limit.show()
```

    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | case_id|province|        city|group|      infection_case|confirmed| latitude| longitude|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | 1000001|   Seoul|  Yongsan-gu| true|       Itaewon Clubs|      139|37.538621|126.992652|
    | 1000002|   Seoul|   Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|
    | 1000003|   Seoul|     Guro-gu| true| Guro-gu Call Center|       95|37.508163|126.884387|
    | 1000004|   Seoul|Yangcheon-gu| true|Yangcheon Table T...|       43|37.546061|126.874209|
    | 1000005|   Seoul|   Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    
    


```python
# column subset select
df_case.select(" case_id","city").show()
df_region.select("city","university_count").show(5)
df_time.select("time","confirmed").show(5)
```

    +--------+---------------+
    | case_id|           city|
    +--------+---------------+
    | 1000001|     Yongsan-gu|
    | 1000002|      Gwanak-gu|
    | 1000003|        Guro-gu|
    | 1000004|   Yangcheon-gu|
    | 1000005|      Dobong-gu|
    | 1000006|        Guro-gu|
    | 1000007|from other city|
    | 1000008|  Dongdaemun-gu|
    | 1000009|from other city|
    | 1000010|      Gwanak-gu|
    | 1000011|   Eunpyeong-gu|
    | 1000012|   Seongdong-gu|
    | 1000013|      Jongno-gu|
    | 1000014|     Gangnam-gu|
    | 1000015|        Jung-gu|
    | 1000016|   Seodaemun-gu|
    | 1000017|      Jongno-gu|
    | 1000018|     Gangnam-gu|
    | 1000019|from other city|
    | 1000020|   Geumcheon-gu|
    +--------+---------------+
    only showing top 20 rows
    
    +-----------+----------------+
    |       city|university_count|
    +-----------+----------------+
    |      Seoul|              48|
    | Gangnam-gu|               0|
    |Gangdong-gu|               0|
    | Gangbuk-gu|               0|
    | Gangseo-gu|               1|
    +-----------+----------------+
    only showing top 5 rows
    
    +----+---------+
    |time|confirmed|
    +----+---------+
    |  16|        0|
    |  16|        0|
    |  16|        0|
    |  16|        1|
    |  16|        0|
    +----+---------+
    only showing top 5 rows
    
    


```python
df_case = df_case.fillna(0)
df_region = df_region.fillna(0)
df_time = df_time.fillna(0)
```


```python
# filters applied 
filtered_cases = df_case.filter((df_case["province"] == "Daegu") & (df_case["confirmed"] > 10))
filtered_cases.show()
```

    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | case_id|province|        city|group|      infection_case|confirmed| latitude| longitude|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    | 1200001|   Daegu|      Nam-gu| true|  Shincheonji Church|     4511| 35.84008|  128.5667|
    | 1200002|   Daegu|Dalseong-gun| true|Second Mi-Ju Hosp...|      196|35.857375|128.466651|
    | 1200003|   Daegu|      Seo-gu| true|Hansarang Convale...|      124|35.885592|128.556649|
    | 1200004|   Daegu|Dalseong-gun| true|Daesil Convalesce...|      101|35.857393|128.466653|
    | 1200005|   Daegu|     Dong-gu| true|     Fatima Hospital|       39| 35.88395|128.624059|
    | 1200008|   Daegu|           -|false|     overseas inflow|       41|        -|         -|
    | 1200009|   Daegu|           -|false|contact with patient|      917|        -|         -|
    | 1200010|   Daegu|           -|false|                 etc|      747|        -|         -|
    +--------+--------+------------+-----+--------------------+---------+---------+----------+
    
    


```python
filter_infection_case = df_case.filter((df_case["infection_case"] == "Day Care Center") | (df_case["province"]=="Daegu"))
filter_infection_case.show()
```

    +--------+--------+---------------+-----+--------------------+---------+---------+----------+
    | case_id|province|           city|group|      infection_case|confirmed| latitude| longitude|
    +--------+--------+---------------+-----+--------------------+---------+---------+----------+
    | 1000005|   Seoul|      Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
    | 1200001|   Daegu|         Nam-gu| true|  Shincheonji Church|     4511| 35.84008|  128.5667|
    | 1200002|   Daegu|   Dalseong-gun| true|Second Mi-Ju Hosp...|      196|35.857375|128.466651|
    | 1200003|   Daegu|         Seo-gu| true|Hansarang Convale...|      124|35.885592|128.556649|
    | 1200004|   Daegu|   Dalseong-gun| true|Daesil Convalesce...|      101|35.857393|128.466653|
    | 1200005|   Daegu|        Dong-gu| true|     Fatima Hospital|       39| 35.88395|128.624059|
    | 1200006|   Daegu|from other city| true|       Itaewon Clubs|        2|        -|         -|
    | 1200007|   Daegu|from other city| true|Cheongdo Daenam H...|        2|        -|         -|
    | 1200008|   Daegu|              -|false|     overseas inflow|       41|        -|         -|
    | 1200009|   Daegu|              -|false|contact with patient|      917|        -|         -|
    | 1200010|   Daegu|              -|false|                 etc|      747|        -|         -|
    +--------+--------+---------------+-----+--------------------+---------+---------+----------+
    
    


```python
filter_region_count = df_region.filter((df_region["city"] == "Seoul") & (df_region["elementary_school_count"] > 10) 
                                       & (df_region["academy_ratio"] > 0.5)).select("city","elementary_school_count","academy_ratio")
filter_region_count.show()
```

    +-----+-----------------------+-------------+
    | city|elementary_school_count|academy_ratio|
    +-----+-----------------------+-------------+
    |Seoul|                    607|         1.44|
    +-----+-----------------------+-------------+
    
    


```python
#sorting the values in descending 
df_case.sort("confirmed",ascending = False).select("confirmed").show()
```

    +---------+
    |confirmed|
    +---------+
    |     4511|
    |      917|
    |      747|
    |      566|
    |      305|
    |      298|
    |      196|
    |      190|
    |      162|
    |      139|
    |      133|
    |      124|
    |      119|
    |      119|
    |      103|
    |      101|
    |      100|
    |       95|
    |       84|
    |       68|
    +---------+
    only showing top 20 rows
    
    


```python
df_case.printSchema()
```

    root
     |--  case_id: integer (nullable = true)
     |-- province: string (nullable = true)
     |-- city: string (nullable = true)
     |-- group: boolean (nullable = true)
     |-- infection_case: string (nullable = true)
     |-- confirmed: integer (nullable = true)
     |-- latitude: string (nullable = true)
     |-- longitude: string (nullable = true)
    
    


```python
# so we can change the datatype of latitude and logitude to double
df_case = df_case.withColumn("latitude",col("latitude").cast(DoubleType()))
df_case = df_case.withColumn("longitude",col("longitude").cast(DoubleType()))
df_case.printSchema()
```

    root
     |--  case_id: integer (nullable = true)
     |-- province: string (nullable = true)
     |-- city: string (nullable = true)
     |-- group: boolean (nullable = true)
     |-- infection_case: string (nullable = true)
     |-- confirmed: integer (nullable = true)
     |-- latitude: double (nullable = true)
     |-- longitude: double (nullable = true)
    
    


```python
#group by province and city with sum of confirmed cases
df_case.groupBy(["province","city"]).sum("confirmed").show()
```

    +----------------+---------------+--------------+
    |        province|           city|sum(confirmed)|
    +----------------+---------------+--------------+
    |Gyeongsangnam-do|       Jinju-si|             9|
    |           Seoul|        Guro-gu|           139|
    |           Seoul|     Gangnam-gu|            18|
    |         Daejeon|              -|           100|
    |    Jeollabuk-do|from other city|             6|
    |Gyeongsangnam-do|Changnyeong-gun|             7|
    |           Seoul|              -|           561|
    |         Jeju-do|from other city|             1|
    |Gyeongsangbuk-do|              -|           345|
    |Gyeongsangnam-do|   Geochang-gun|            18|
    |Gyeongsangbuk-do|        Gumi-si|            10|
    |         Incheon|from other city|           117|
    |           Busan|              -|            85|
    |           Daegu|         Seo-gu|           124|
    |           Busan|     Suyeong-gu|             5|
    |     Gyeonggi-do|   Uijeongbu-si|            50|
    |           Seoul|     Yongsan-gu|           139|
    |           Daegu|              -|          1705|
    |           Seoul|   Seodaemun-gu|             5|
    |     Gyeonggi-do|    Seongnam-si|            94|
    +----------------+---------------+--------------+
    only showing top 20 rows
    
    


```python
#joining two dataframes one with cases and region
# showing two ways we can show the joins 
#df_covid_case_region = df_case.join(df_region,df_region.province == df_case.province,"inner")
df_covid_case_region_inner = df_case.join(df_region,"province","inner")
df_covid_case_region_left = df_case.join(df_region,"province","left")
df_covid_case_region_right = df_case.join(df_region,"province","right")
df_covid_case_region_outer = df_case.join(df_region,"province","outer")
df_covid_case_region_inner.show(5)
df_covid_case_region_left.show(5)
df_covid_case_region_right.show(5)
df_covid_case_region_outer.show(5)
```

    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |province| case_id|   city|group|      infection_case|confirmed|         latitude|        longitude| code| city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |   Seoul| 1000038|      -|false|                 etc|      100|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000037|      -|false|contact with patient|      162|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000036|      -|false|     overseas inflow|      298|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000035|Guro-gu| true|     Daezayeon Korea|        3|37.48683547973633|126.8931655883789|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000034|      -| true|         Orange Life|        1|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    only showing top 5 rows
    
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |province| case_id|      city|group|infection_case|confirmed|         latitude|        longitude| code|        city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |   Seoul| 1000001|Yongsan-gu| true| Itaewon Clubs|      139|37.53861999511719|126.9926528930664|10250| Jungnang-gu|37.606832|127.092656|                     23|                31|               1|          0.7|                   16.65|                6.9|               689|
    |   Seoul| 1000001|Yongsan-gu| true| Itaewon Clubs|      139|37.53861999511719|126.9926528930664|10240|     Jung-gu|37.563988| 126.99753|                     12|                14|               2|         0.94|                   18.42|                7.4|               728|
    |   Seoul| 1000001|Yongsan-gu| true| Itaewon Clubs|      139|37.53861999511719|126.9926528930664|10230|   Jongno-gu|37.572999|126.979189|                     13|                17|               3|         1.71|                   18.27|                6.8|               668|
    |   Seoul| 1000001|Yongsan-gu| true| Itaewon Clubs|      139|37.53861999511719|126.9926528930664|10220|Eunpyeong-gu|37.603481|126.929173|                     31|                44|               1|         1.09|                    17.0|                6.5|               874|
    |   Seoul| 1000001|Yongsan-gu| true| Itaewon Clubs|      139|37.53861999511719|126.9926528930664|10210|  Yongsan-gu|37.532768|126.990021|                     15|                13|               1|         0.68|                   16.87|                6.5|               435|
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    only showing top 5 rows
    
    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |province| case_id|   city|group|      infection_case|confirmed|         latitude|        longitude| code| city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |   Seoul| 1000038|      -|false|                 etc|      100|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000037|      -|false|contact with patient|      162|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000036|      -|false|     overseas inflow|      298|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000035|Guro-gu| true|     Daezayeon Korea|        3|37.48683547973633|126.8931655883789|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    |   Seoul| 1000034|      -| true|         Orange Life|        1|             NULL|             NULL|10000|Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
    +--------+--------+-------+-----+--------------------+---------+-----------------+-----------------+-----+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    only showing top 5 rows
    
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |province| case_id|      city|group|infection_case|confirmed|         latitude|        longitude| code|        city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    |   Busan| 1100001|Dongnae-gu| true| Onchun Church|       39|35.21628189086914|129.0771026611328|11000|       Busan|35.179884|129.074796|                    304|               408|              22|          1.4|                   18.41|                8.6|              6752|
    |   Busan| 1100001|Dongnae-gu| true| Onchun Church|       39|35.21628189086914|129.0771026611328|11010|  Gangseo-gu|35.212424| 128.98068|                     17|                21|               0|         1.43|                   11.84|                5.0|               147|
    |   Busan| 1100001|Dongnae-gu| true| Onchun Church|       39|35.21628189086914|129.0771026611328|11020|Geumjeong-gu|35.243053|129.092163|                     22|                28|               4|         1.64|                    19.8|                8.4|               466|
    |   Busan| 1100001|Dongnae-gu| true| Onchun Church|       39|35.21628189086914|129.0771026611328|11030|  Gijang-gun|35.244881|129.222253|                     21|                35|               0|         1.31|                   15.45|                7.4|               229|
    |   Busan| 1100001|Dongnae-gu| true| Onchun Church|       39|35.21628189086914|129.0771026611328|11040|      Nam-gu|35.136789| 129.08414|                     21|                27|               4|         1.24|                   19.13|                7.9|               475|
    +--------+--------+----------+-----+--------------+---------+-----------------+-----------------+-----+------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
    only showing top 5 rows
    
    


```python
#sql in pyspark showing province wise confirmed  cases greater then 100 using group by
df_case.registerTempTable("cases")
df_case_sql = spark.sql("select province,SUM(confirmed) as total_confirmed_cases from cases GROUP by province having total_confirmed_cases > 100")
df_case_sql.show()
```

    +-----------------+---------------------+
    |         province|total_confirmed_cases|
    +-----------------+---------------------+
    | Gyeongsangbuk-do|                 1324|
    |            Daegu|                 6680|
    | Gyeongsangnam-do|                  132|
    |          Incheon|                  202|
    |      Gyeonggi-do|                 1000|
    |            Busan|                  156|
    |          Daejeon|                  131|
    |            Seoul|                 1280|
    |Chungcheongnam-do|                  158|
    +-----------------+---------------------+
    
    


```python
@udf(returnType=StringType())
def casehighlow(case):
    if(case > 100):
        return "high"
    else:
        return "low"

df_case.select("province","city","confirmed",casehighlow("confirmed").alias("Cases_high_low")).show(10)

```

    +--------+---------------+---------+--------------+
    |province|           city|confirmed|Cases_high_low|
    +--------+---------------+---------+--------------+
    |   Seoul|     Yongsan-gu|      139|          high|
    |   Seoul|      Gwanak-gu|      119|          high|
    |   Seoul|        Guro-gu|       95|           low|
    |   Seoul|   Yangcheon-gu|       43|           low|
    |   Seoul|      Dobong-gu|       43|           low|
    |   Seoul|        Guro-gu|       41|           low|
    |   Seoul|from other city|       36|           low|
    |   Seoul|  Dongdaemun-gu|       17|           low|
    |   Seoul|from other city|       25|           low|
    |   Seoul|      Gwanak-gu|       30|           low|
    +--------+---------------+---------+--------------+
    only showing top 10 rows
    
    


```python

```


```python

```
