import os
import sys

# Path for spark source folder
sparkpath = "/Users/eyalbenivri/Developer/libs/spark-1.6.1"

os.environ['SPARK_HOME'] = sparkpath

# Append pyspark to Python Path
sys.path.append(os.environ['SPARK_HOME'] + "/python")
sys.path.append(os.environ['SPARK_HOME'] + "/python/lib/py4j-0.9-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    datadir = "/Users/eyalbenivri/Developer/projects/spark-workshop/data/"

    # Task 1: Implement a word count program to read all *.txt files from the data directory and count all the words there
    print("------- ******* Task 1 ******* -------")
    lines = sc.textFile(datadir + "*.txt")
    words = lines.flatMap(lambda line: line.split())
    pairs = words.map(lambda word: (word, 1))
    freqs = pairs.reduceByKey(lambda a, b: a + b)
    top10 = freqs.sortBy(lambda (word, count): -count).take(10)
    for (word, count) in top10:
        print("the word '%s' appears %d times" % (word, count))

    # Task 2: Revisit the program to filter out words shorter the 4 characters
    print("------- ******* Task 2 ******* -------")
    filtered_words = words.filter(lambda word: len(word) >= 4)
    pairs = filtered_words.map(lambda word: (word, 1))
    freqs = pairs.reduceByKey(lambda a, b: a + b)
    top10 = freqs.sortBy(lambda (word, count): -count).take(10)
    for (word, count) in top10:
        print("the word '%s' appears %d times" % (word, count))

    # Task 3: call freqs.toDebugString method, print the resulted string and explain the execution plan for that variable
    print("------- ******* Task 3 ******* -------")
    print(freqs.toDebugString())