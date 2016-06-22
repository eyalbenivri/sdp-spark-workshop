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
    from pyspark import SQLContext
    from pyspark.sql.types import IntegerType

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    datadir = "/YOUR/DATA/DIR/"

    # head -n 1 ~/data/companies.json
    sqlContext = SQLContext(sc)

    # Task 1: Read and parse the data using sqlContext, print the schema and register the data as a temp table
    print("------- ******* Task 1 ******* -------")

    # Task 2: Let's talk about the money; figure out what the average acquisition price was.
    print("------- ******* Task 2 ******* -------")

    # Task 3:  Let's get some additional detail
    # print the average acquisition price grouped by number of years the company was active
    print("------- ******* Task 3 ******* -------")

    # Task 4: let's try to figure out the relationship between the company's total funding and acquisition price.
    # In order to do that, you'll need a UDF (user-defined function) that, given a company,
    # returns the sum of all its funding rounds.
    # First, build that function and register it with the name "total_funding".
    print("------- ******* Task 4 ******* -------")

    # Task 5: Test your function by retrieving the total funding for a few companies,
    # such as Facebook, Paypal, and Alibaba.
    # Now, find the average ratio between the acquisition price and the total funding
    # which, in a simplistic way, represents return on investment.
    print("------- ******* Task 5 ******* -------")
    