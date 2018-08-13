#import findspark
#findspark.init()

import sys
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import mean, asc
sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

inputFile = sys.argv[1]
ratings = sc.textFile(inputFile) 
rdd = ratings.zipWithIndex().filter(lambda (row,index): index > 0).keys()
#rdd.take(5)
rdd2 = rdd.map(lambda x:x.split(','))
rdd3_mapped = rdd2.map(lambda y : (int(y[1]),float(y[2])))
#rdd3_mapped.take(10)
aTuple = (0.0,0)
rdd1 = rdd3_mapped.aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),
lambda a,b: (a[0] + b[0], a[1] + b[1]))
#rdd1.take(10)
rddTemp = rdd1.sortByKey(True)
finalResult = rddTemp.mapValues(lambda v: v[0]/v[1])
#print(finalResult)
df = finalResult.toDF(["movieId","rating_avg"])
outputFolder = sys.argv[2]
df.coalesce(1).write.csv(outputFolder,header=True)