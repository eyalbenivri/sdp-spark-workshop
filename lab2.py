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

    datadir = "YOUR/DATA/DIR/"

    # wc -l airline-delays.csv
    # cat airline-delays.csv | cut -d',' -f1-20 | awk '{ if (rand() <= 0.00005 || FNR==1) { print $0; if (++count > 11) exit; } }'

    # python has a csv parsing lib (not related to spark)
    # try it out in the python REPL or in pyspark

    import csv
    from StringIO import StringIO


    #
    # si = StringIO('"Alice",14,"panda"')
    # fields = ["name", "age", "favorite animal"]
    # csv.DictReader(si, fieldnames=fields).next()

    # Task 1: write a function that parses one line from the flight delays CSV file.
    # You can call that function parseLine, and it should return the Python dict that DictReader.next returns.
    print("------- ******* Task 1 ******* -------")

    # Task 2: create an RDD based on the airline-delays.csv file,
    # and map each line of that file using the parseLine function you wrote.
    # The result should be an RDD of Python dicts representing the flight delay data.
    # Note that the first line (the header line) should be discarded.
    print("------- ******* Task 2 ******* -------")

    # Task 3: Now that you have the flight objects,
    # it's time to perform a few queries and gather some useful information.
    # Suppose you're in Boston, MA. Which airline has the most flights departing from Boston?
    # Hint: OriginCityName == "Boston, MA"
    print("------- ******* Task 3 ******* -------")

    # Task 4: Overall, which airline has the worst average delay? How bad was that delay?
    print("------- ******* Task 4 ******* -------")

    # Task 5: Living in Chicago, IL, what are the farthest 10 destinations that you could fly to?
    # (Note that our dataset contains only US domestic flights.)
    print("------- ******* Task 5 ******* -------")

    # Task 6: Suppose you're in New York, NY and are contemplating direct flights to San Francisco, CA.
    # In terms of arrival delay, which airline has the best record on that route?
    print("------- ******* Task 6 ******* -------")

