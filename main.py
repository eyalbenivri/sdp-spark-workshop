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
    # conf.setMaster("local[2]")
    # conf.setAppName("SparkDemo")
    # conf.set("spark.executor.memory", "8g")
    sc = SparkContext(conf=conf)

    
