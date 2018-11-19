import os,sys
from pyspark import SparkContext, SparkConf
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from commons.Utils import Utils
sys.path.insert(0, '.')

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("C:/Users/lprda/python-spark-tutorial/in/airports.text")
    print(airports)
    airportsInUSA = airports.filter(lambda line : Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("C:/Users/lprda/python-spark-tutorial/out/airports_in_usa")

