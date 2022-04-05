package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.immutable._
import plotly._, element._, layout._, Plotly._
import java.io._


object Analysis {

    /**
     * This app reads csv files from the datalake and transforms them to dataframes to perform analysis 
     */
    def main(args: Array[String]) {
        
        // Create a SparkSession
        val spark = SparkSession
            .builder()
            .appName("PeaceLand")
            .master("local[*]")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._
        val sqlContext = spark.sqlContext

        // directory path of the datalakes csv files
        val DATALAKE_PATH = "../PeaceLand_DataLake/data/data/part-00000*.csv"

        // Create a schema for our dataframe
        val df_schema = new StructType()
            .add("id", StringType)
            .add("name", StringType)
            .add("age", StringType)
            .add("emotion", StringType)
            .add("behavior", StringType)
            .add("pscore", StringType)
            .add("datetime", StringType)
            .add("lat", StringType)
            .add("lon", StringType)
            .add("words", StringType)
            
        // read csv files from the datalake
        val df = sqlContext.read.format("csv")
            .option("header", "false")
            .option("inferSchema", "true")
            .schema(df_schema)
            .load(DATALAKE_PATH)
        
        
        // show the schema of the dataframe
        df.printSchema()

        // show the first 10 rows of the dataframe

        print("\n \n ************* Apercu du DataFrame ************* \n \n")
        df.show(10)

        // count the number of rows in the dataframe
        print("\n \n ************* Nombre de lignes dans le DataFrame ************* \n \n")
        
        print("Nombre : " ,df.count())

        // classify names in dataframe by emotion
        print("\n \n ************* Classification par emotion ************* \n \n")
        df.groupBy("emotion").count().show()

        // show names in dataframe by emotion
        print("\n \n ************* Noms par emotion ************* \n \n")
        df.groupBy("emotion").agg(collect_list("name").alias("names")).show()

        // group words of each name by emotion
        print("\n \n ************* Groupement par emotion ************* \n \n")
        df.groupBy("emotion").agg(collect_list("words").alias("words")).show()

        // show all words of each name
        print("\n \n ************* Noms ************* \n \n")
        df.groupBy("name").agg(collect_list("words").alias("words")).show()

        // show names of people who are angry,afraid,alarmed, depressed
        print("\n \n ************* Noms des personnes qui sont en Bad mood ************* \n \n")
        
        df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").agg(collect_list("words").alias("words")).show()
        // count the number of people in bad mood
        print("\n \n ************* Nombre de personnes en Bad mood ************* \n \n")
        df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").count().show()

        // show names of people who are happy
        print("\n \n ************* Noms des personnes qui sont en Good mood ************* \n \n")
      
        df.filter(df("emotion") === "excited" || df("emotion") === "delighted" || df("emotion") === "pleased" || df("emotion") === "calm" || df("emotion")==="relax" || df("emotion")==="content").groupBy("name").agg(collect_list("words").alias("words")).show()
        // count the number of people in good mood
        print("\n \n ************* Nombre de personnes en Good mood ************* \n \n")
        df.filter(df("emotion") === "excited" || df("emotion") === "delighted" || df("emotion") === "pleased" || df("emotion") === "calm" || df("emotion")==="relax" || df("emotion")==="content").count()

        // position of people who are angry,afraid,alarmed, depressed
        print("\n \n ************* Position des personnes qui sont en Bad mood ************* \n \n")
        df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").agg(collect_list("lat").alias("lat"), collect_list("lon").alias("lon")).show()
        
        // Show the number of people watched by a peacewatcher
        print("\n \n ************* Nombre peacewatched by peacewatcher  ************* \n \n")
        df.groupBy("id").count().show()

        // show the peacewatcher with the highest count peacewatched
        print("\n \n ************* ID ************* \n \n")
        df.groupBy("id").count().sort(desc("count")).show()
        
    }
}