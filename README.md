In order to run the pyspark code, please do the following steps:
1. install Spark and Hadoop(I used Spark2.0.2 against Hadoop-2.7.2)
2. copy file from local to HDFS, command is following
/path/to/hadoop/bin/hdfs dfs -copyFromLocal /path/to/data/file/getatat_test.csv /path/of/your/choice/getatat_test.csv
3. start pyspark in command line as following(I started spark with yarn mode, you can choose any mode you want)
/path/to/spark/pyspark --master yarn-client
4. copy all the command from file getstat.py to command line and see the result.

Based on my query result, my answers for the questions are:
1. Which URL has the most ranks below 10?
   A: serpbook.com/
2. Which rank 1 keyword(s) change URLs the most?
   A: |    299|       google update|
      |    188|        voice search|
      |    188|      digital agency|
3. Are the top 10 rankings the same across “desktop“ and “smartphone” devices?
   A: yes
