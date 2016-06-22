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
    from pyspark import SQLContext
    from pyspark.sql.types import IntegerType

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == '__main__':
    conf = SparkConf()

    sc = SparkContext(conf=conf)

    datadir = "/Users/eyalbenivri/Developer/projects/spark-workshop/data/"

    # sudo dpkg --configure -a
    # sudo apt-get install python-setuptools
    # sudo easy_install dateutils
    # Download pyspark_csv.py from https://github.com/seahboonsiew/pyspark-csv
    sys.path.append('/Users/eyalbenivri/Developer/libs/pyspark_libs')  # replace as necessary
    import pyspark_csv

    sc.addFile('/Users/eyalbenivri/Developer/libs/pyspark_libs/pyspark_csv.py')  # ditto
    sqlContext = SQLContext(sc)

    # Task 1: load the prop-prices.csv file as an RDD, and use the csvToDataFrame function from the pyspark_csv module
    # to create a DataFrame and register it as a temporary table so that you can run SQL queries:
    print("------- ******* Task 1 ******* -------")
    columns = ['id', 'price', 'date', 'zip', 'type', 'new', 'duration', 'PAON',
               'SAON', 'street', 'locality', 'town', 'district', 'county', 'ppd',
               'status']

    rdd = sc.textFile(datadir + "prop-prices.csv")
    df = pyspark_csv.csvToDataFrame(sqlContext, rdd, columns=columns)
    df.registerTempTable("properties")
    df.persist()

    # Task 2: let's do some basic analysis on the data.
    # Find how many records we have per year, and print them out sorted by year.
    print("------- ******* Task 2 ******* -------")
    year_count = sqlContext.sql(
        """select   year(date) as year, count(*) as count
        from     properties
        group by year(date)
        order by year(date)""").collect()
    print(year_count)

    # Task 3: Everyone knows that properties in London are expensive.
    # Find the average property price by county,
    # and print the top 10 most expensive counties
    print("------- ******* Task 3 ******* -------")
    county_prices = sqlContext.sql(
        """select   county, avg(price) as avg_price
        from     properties
        group by county
        order by avg(price) desc
        limit    10""").collect()
    print(county_prices)

    # Task 4: Is there any trend for property sales during the year?
    # Find the average property price in Greater London month over month in 2015 and 2016,
    # and print it out by month.
    print("------- ******* Task 4 ******* -------")
    avg_price_by_month = sqlContext.sql(
        """select   year(date) as year, month(date) as month, avg(price) as avg_price
        from     properties
        where    county='GREATER LONDON'
            and      year(date) >= 2015
        group by year(date), month(date)
        order by year(date), month(date)""").collect()
    print(avg_price_by_month)

    # Bonus Task: Install matplot lib: sudo easy_install matplotlib
    # Use the Python matplotlib module to plot the property price
    # changes month-over-month across the entire dataset.
    print("------- ******* Bonus Task ******* -------")
    import matplotlib.pyplot as plt
    monthPrices = sqlContext.sql("""select   year(date) as year, month(date) as month, avg(price) as avg_price
                                    from     properties
                                    group by year(date), month(date)
                                    order by year(date), month(date)""").collect()

    values = map(lambda row: (str(row.year) + "-" + str(row.month), row.avg_price), monthPrices)
    x = map(lambda (time, avg_price): time, values)
    y = map(lambda (time, avg_price): avg_price, values)
    plt.rcdefaults()
    plt.scatter(xrange(0, len(x)), y)
    plt.show()


