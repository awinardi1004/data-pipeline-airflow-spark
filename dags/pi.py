from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PiCalc").getOrCreate()

def f(_):
    import random
    x = random.random()
    y = random.random()
    return 1 if x*x + y*y < 1 else 0

count = spark.sparkContext.parallelize(range(0, 100000)).map(f).reduce(lambda a, b: a + b)
print("Pi is roughly %f" % (4.0 * count / 100000))

spark.stop()
