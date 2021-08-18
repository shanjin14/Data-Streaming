## The code contains both the RDD version and the DataFrame version

from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("HelloSpark2")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt

logFile = "/home/workspace/Test.txt"
# TO-DO: create a Spark session
#spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TO-DO: set the log level to WARN
#spark.sparkContext.setLogLevel("WARN")

#logData=spark.read.text(logFile).cache()
logDataRDD = sc.textFile(logFile)


# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'a' has been encountered (including in this row)
#numACnt=logData.filter(logData.value.contains('a')).count()
acntRDD = logDataRDD.map(lambda s: [c for word in s for c in word].count('a'))
numACnt = acntRDD.reduce(lambda a,b:a+b)

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'b' has been encountered (including in this row)
bcntRDD = logDataRDD.map(lambda s: [c for word in s for c in word].count('b'))
numBCnt = bcntRDD.reduce(lambda a,b:a+b)
#numBCnt=logData.filter(logData.value.contains('b')).count()


# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found


# TO-DO: print the count for letter 'a' and letter 'b'
print(f"cnt a:{numACnt} ; cnt b:{numBCnt}")
# TO-DO: stop the spark application
#spark.stop()
sc.stop()
