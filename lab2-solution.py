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
    def parseLine(line, fieldnames):
        si = StringIO(line)
        return csv.DictReader(si, fieldnames=fieldnames).next()


    # Task 2: create an RDD based on the airline-delays.csv file,
    # and map each line of that file using the parseLine function you wrote.
    # The result should be an RDD of Python dicts representing the flight delay data.
    # Note that the first line (the header line) should be discarded.
    rdd = sc.textFile(datadir + "airline-delays.csv")
    headerline = rdd.first()
    splitheaders = map(lambda field: field.strip('"'), headerline.split(','))
    fieldnames = filter(lambda field: len(field) > 0, splitheaders)
    flights = rdd.filter(lambda line: line != headerline).map(lambda line: parseLine(line, fieldnames))
    flights.persist()

    # Task 3: Now that you have the flight objects,
    # it's time to perform a few queries and gather some useful information.
    # Suppose you're in Boston, MA. Which airline has the most flights departing from Boston?
    # Hint: OriginCityName == "Boston, MA"
    print("------- ******* Task 3 ******* -------")
    flightsByCarrier = flights.filter(lambda flight: flight['OriginCityName'] == "Boston, MA") \
        .map(lambda flight: flight['Carrier']) \
        .countByValue()

    flightssorted = sorted(flightsByCarrier.items(), key=lambda p: -p[1])[0]
    print(flightssorted)

    # Task 4: Overall, which airline has the worst average delay? How bad was that delay?
    print("------- ******* Task 4 ******* -------")
    carriersDelays = flights.filter(lambda f: f['ArrDelay'] != '').map(lambda f: (f['Carrier'], float(f['ArrDelay'])))
    combined = carriersDelays.combineByKey(lambda d: (d, 1),
                                           lambda s, d: (s[0] + d, s[1] + 1),
                                           lambda s1, s2: (s1[0] + s2[0], s1[1] + s2[1]))
    print(combined.map(lambda (k, (s, c)): (k, s / float(c))).collect())

    # Task 5: Living in Chicago, IL, what are the farthest 10 destinations that you could fly to?
    # (Note that our dataset contains only US domestic flights.)
    print("------- ******* Task 5 ******* -------")
    chicago = flights.filter(lambda f: f['OriginCityName'] == "Chicago, IL") \
        .map(lambda f: (f['DestCityName'], float(f['Distance']))) \
        .distinct() \
        .sortBy(lambda (dest, dist): -dist) \
        .take(10)
    print(chicago)

    # Task 6: Suppose you're in New York, NY and are contemplating direct flights to San Francisco, CA.
    # In terms of arrival delay, which airline has the best record on that route?
    print("------- ******* Task 6 ******* -------")
    newyork = flights.filter(lambda flight: flight['OriginCityName'] == "New York, NY" and
                                  flight['DestCityName'] == "San Francisco, CA" and
                                  flight['ArrDelay'] != '') \
        .map(lambda flight: (flight['Carrier'], float(flight['ArrDelay']))) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda (carrier, delay): delay) \
        .first()
    print(newyork)
