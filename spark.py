from pandas import concat
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col, lit, concat, collect_list
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from nltk.metrics import jaccard_distance

threshold = .7 #for calculating similarity measure

# calculting similarity value for matching stage. 
def calculateSimilarity(acmPublicationTitle:str, dblpPublicationTitle:str)-> float|None:
    set1 = set(str(acmPublicationTitle).lower().split())
    set2 = set(str(dblpPublicationTitle).lower().split())
    # Calculate Jaccard similarity using similaritymeasures
    # jaccard_similarity = similaritymeasures.Jaccard(set1,set2)
    jaccard_similarity = 1.0 - jaccard_distance(set1,set2)
    return jaccard_similarity



def sparkER():

    # # working with sample data url
    # urlACM = "data/sampleACM.csv"
    # urlDBLP = "data/sampleDBLP.csv"

    # real data url
    urlACM = "data/ACM_1995_2004.csv"
    urlDBLP = "data/DBLP_1995_2004.csv"

    # creating spark session
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName('erSparkJob') \
        .getOrCreate()

    schema = StructType() \
            .add("publicationTitle", StringType()) \
            .add("Authors", StringType()) \
            .add("yearOfPublication", IntegerType()) \
            .add("publicationVenue", StringType()) \
            .add("publicationID", StringType())


    # reading ACM data using dataframe
    dfACM = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(urlACM)
    print("print sample of dfACM after reading")
    dfACM.show(5)


    # reading DBLP data using dataframe
    dfDBLP = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(urlDBLP)
    print("print sample of  dfDBLP after reading")
    dfDBLP.show(5)

    #********************************* BLOCKING STAGE  *********************************


    # creating new col 'key' to create dict
    dfACM = dfACM.withColumn("key", concat(col("publicationVenue"), col("yearOfPublication").cast("string")))
    dfDBLP = dfDBLP.withColumn("key", concat(col("publicationVenue"), col("yearOfPublication").cast("string")))
    
    # Alias the columns of dfDBLP to have different col name when joining later.
    dfDBLP_alias = dfDBLP.select([col(column).alias(f"dblp_{column}") for column in dfDBLP.columns])
    print("checking dblp col name")
    # dfDBLP_alias.show(5)

    # joining df based on the key. that way we will have the blocking as they will only join where the key is same. 
    dfCombine = dfACM.join(dfDBLP_alias, dfACM["key"] == dfDBLP_alias["dblp_key"], "inner")
    print("showing joined col of acm and dblp sample")
    dfCombine.show(6)
    #********************************* MATCHING STAGE  *********************************

    # in matching stage we will use the col publicationTitle and dblp_publicationTitle Col of dfCombine to find similarity scores. 

    # Register the UDF
    calculateSimilarity_udf = udf(calculateSimilarity, FloatType())

    # Assuming df is your Spark DataFrame and you want to create a new column based on columns 'col1' and 'col2'
    dfCombine = dfCombine.withColumn("similarityScore", calculateSimilarity_udf(dfCombine['publicationTitle'], dfCombine['dblp_publicationTitle']))
    print("showing sample dfCombine with similarity scores")
    dfCombine.show(6)

    # filtering based on threshold value 
    dfCombine = dfCombine.filter(dfCombine['similarityScore'] >= threshold)
    dfCombine.show(5)

    print("saving dataframe in csv format in data/Matched Entities with Spark.csv ")
    dfCombine.write.csv("data/Matched Entities with Spark.csv", header=True, mode="overwrite")
    print("saving finished")








    # Stop SparkSession
    spark.stop()


if __name__ == "__main__":
    sparkER()
    



