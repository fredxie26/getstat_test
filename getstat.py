from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#load file from HDFS and get rid of the CSV header
text_file = sc.textFile("/user/root/getatat_test.csv").filter(lambda l: not l.startswith("Keyword"))

#create table schema
dataSchema = StructType([StructField("Keyword", StringType(), True),
                         StructField("Market", StringType(), True),
                         StructField("Location", StringType(), True),
                         StructField("Device", StringType(), True),
                         StructField("CrawlDate", StringType(), True),
                         StructField("Rank", StringType(), True),
                         StructField("URL", StringType(), True)]
                         )

#load data into DataFrame and register into spark Temp table                       
data = text_file.map(lambda s: s.split(",")).map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6]))
data_df = spark.createDataFrame(data, dataSchema)  
data_df.registerTempTable("data")

#find the top URL count below rank 10
sqlContext.sql("SELECT count(*) AS urlcounts, URL FROM data WHERE Rank > 10 group by URL ORDER BY urlcounts DESC").show(10)

#find the URl with rank number 1 and is different from all the URLs in previous date group by keyword
spark.conf.set("spark.sql.crossJoin.enabled", "true")
sqlContext.sql("SELECT count(*) AS Changes, Keyword FROM data WHERE URL NOT IN (SELECT URL FROM data WHERE CrawlDate IN (SELECT date_sub(TO_DATE(CAST(UNIX_TIMESTAMP(CrawlDate, 'yyyy-mm-dd') AS TIMESTAMP)), 1) AS previous FROM data)) AND rank = 1 GROUP BY Keyword ORDER BY Changes DESC").show(10)

#find if there is any Device column that is not desktop or smartphone in top 10 tank
sqlContext.sql("SELECT * FROM data WHERE Rank < 11 AND Device NOT IN ('desktop', 'smartphone')").show(10)
