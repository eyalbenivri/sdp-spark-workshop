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

    # sudo dpkg --configure -a
    # sudo apt-get install python-setuptools
    # sudo easy_install dateutils
    # Download pyspark_csv.py from https://github.com/seahboonsiew/pyspark-csv
    sys.path.append('/YOUR/PYSPARK_LIBS/DIR')  # replace as necessary
    import pyspark_csv

    sc.addFile('/YOUR/PYSPARK_LIBS/DIR/pyspark_csv.py')  # ditto
    sqlContext = SQLContext(sc)

    # Task 1: load the prop-prices.csv file as an RDD, and use the csvToDataFrame function from the pyspark_csv module
    # to create a DataFrame and register it as a temporary table so that you can run SQL queries:
    print("------- ******* Task 1 ******* -------")

    # Task 2: let's do some basic analysis on the data.
    # Find how many records we have per year, and print them out sorted by year.
    print("------- ******* Task 2 ******* -------")

    # Task 3: Everyone knows that properties in London are expensive.
    # Find the average property price by county,
    # and print the top 10 most expensive counties
    print("------- ******* Task 3 ******* -------")

    # Task 4: Is there any trend for property sales during the year?
    # Find the average property price in Greater London month over month in 2015 and 2016,
    # and print it out by month.
    print("------- ******* Task 4 ******* -------")

    # Bonus Task: Install matplot lib: sudo easy_install matplotlib
    # Use the Python matplotlib module to plot the property price
    # changes month-over-month across the entire dataset.
    print("------- ******* Bonus Task ******* -------")
    import matplotlib.pyplot as plt

    # plt.rcdefaults()
    # plt.scatter(x, y)
    # plt.show()


