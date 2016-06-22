import os
import sys

# Path for spark source folder
sparkpath = "/home/spark/Downloads/spark-161"

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

    datadir = "/YOUR/DATA/DIR/"

    # Task 1: Implement a word count program to read all *.txt files from the data directory and count all the words there
    print("------- ******* Task 1 ******* -------")

    # Task 2: Revisit the program to filter out words shorter the 4 characters
    print("------- ******* Task 2 ******* -------")

    # Task 3: call freqs.toDebugString method, print the resulted string and explain the execution plan for that variable
    print("------- ******* Task 3 ******* -------")
