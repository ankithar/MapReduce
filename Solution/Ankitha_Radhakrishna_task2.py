#import findspark
#findspark.init()

import sys
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import mean, desc, asc
sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)


#ratingsfile = 'C:\\Users\\ankit\\Desktop\\tmp\\ratings.csv'
ratingsfile = sys.argv[1]
ratings = sc.textFile(ratingsfile)
rddRatings = ratings.zipWithIndex().filter(lambda (row,index): index > 0).keys()
#rddRatings.take(5)
rddRatingsMap = rddRatings.map(lambda x:x.split(','))
rddRatingsMap_mapped = rddRatingsMap.map(lambda y : (y[1],float(y[2])))
#rddRatingsMap_mapped.take(10)

#tagsfile = 'C:\\Users\\ankit\\Desktop\\tmp\\tags.csv'
tagsfile = sys.argv[2]
tags = sc.textFile(tagsfile)
rddTags = tags.zipWithIndex().filter(lambda (row,index): index > 0).keys()
#rddTags.take(5)
rddTagsMap = rddTags.map(lambda x:x.split(','))
rddTagsMap_mapped = rddTagsMap.map(lambda y : (y[2],y[1]))
#rddTagsMap_mapped.take(10)

dfRatings = rddRatingsMap_mapped.toDF(["movie_id","ratings"])
#dfRatings.show(10)
dfTags = rddTagsMap_mapped.toDF(["tag","movie_id"])
#dfTags.show(10)

ta = dfRatings.alias('ta')
tb = dfTags.alias('tb')

inner_join = ta.join(tb, ta.movie_id == tb.movie_id)
#inner_join.show()

#inner_join.groupBy("tag").agg(mean("ratings").alias("average_rating")).orderBy(desc("average_rating")).take(20)
dfResult = inner_join.groupBy("tag").agg(mean("ratings").alias("rating_avg")).orderBy(desc("tag"))

outputFile = sys.argv[3]
dfResult.coalesce(1).write.csv(outputFile,header=True)