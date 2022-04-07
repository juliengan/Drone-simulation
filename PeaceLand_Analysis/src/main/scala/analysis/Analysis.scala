package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


import scala.collection.immutable._
import plotly._
import element._
//import layout._
//import Plotly._
//import io.netty.handler.codec.mqtt.MqttMessageBuilders.publish
//import org.apache.log4j.{Level, Logger}
//import org.scalajs.dom
//import org.scalajs.dom.document
import shapeless.Lazy.apply

import java.io._
//import scala.scalajs.js.annotation.JSExportTopLevel
//import scala.scalajs.js

object Analysis {

    /**
     * This app reads csv files from the datalake and transforms them to dataframes to perform analysis 
     */
    def main(args: Array[String]): Unit = {
        val df = loadData()
        //df.show()
        emotionByMostCommon(df)

        //htmlUpdate()
        // show the schema of the dataframe
        //df.printSchema()

        // show the first 10 rows of the dataframe
        //print(happyCitizens(df))
        //print(namesByEmotion(df))
    }


    /*def htmlUpdate(): Unit = {
        document.addEventListener("DOMContentLoaded", { (e: dom.Event) =>
            setupUI()
        })
    }

    def setupUI(): Unit = {
        val button = document.createElement("button")
        button.textContent = "Click me!"
        button.addEventListener("click", { (e: dom.MouseEvent) =>
            addClickedMessage()
        })
        document.body.appendChild(button)
        appendPar(document.body, "Hello World")
    }

    @JSExportTopLevel("addClickedMessage")
    def addClickedMessage(): Unit = {
        appendPar(document.body, "You clicked the button!")
    }

    def appendPar(targetNode: dom.Node, text: String): Unit = {
        val parNode = document.createElement("p")
        parNode.textContent = text
        targetNode.appendChild(parNode)
    }*/

    def loadData(): DataFrame ={
        // Create a SparkSession
        val spark = SparkSession
          .builder()
          .appName("PeaceLand")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("FATAL")

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
        sqlContext.read.format("csv")
          .option("header", "false")
          .option("inferSchema", "true")
          .schema(df_schema)
          .load(DATALAKE_PATH)
    }

    def nbRows(df : DataFrame): Long ={
        print("\n \n ************* Nombre de lignes dans le DataFrame ************* \n \n")
        df.count()
    }

    def emotionByMostCommon(df : DataFrame): Unit ={
        print("\n \n ************* Classification par emotion ************* \n \n")
        df.groupBy("emotion").count().show()
    }

    def namesByEmotion(df : DataFrame): Unit ={
        print("\n \n ************* Noms par emotion ************* \n \n")
        df.groupBy("emotion").agg(collect_list("name").alias("names")).show()
    }

    def wordsNameByEmotion(df : DataFrame): Unit ={
        df.groupBy("emotion").agg(collect_list("words").alias("words")).show()
    }

    def wordsOfCitizen(df : DataFrame): Unit ={
        df.groupBy("name").agg(collect_list("words").alias("words")).show()
    }

    def nbSadCitizens(df : DataFrame): Unit ={
        df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").count().show()
    }

    // show names of people who are happy
    def happyCitizens(df : DataFrame): DataFrame ={
        df.filter(df("emotion") === "excited" || df("emotion") === "delighted" || df("emotion") === "pleased" || df("emotion") === "calm" || df("emotion")==="relax" || df("emotion")==="content").groupBy("name").agg(collect_list("words").alias("words"))
        //print("\n \n ************* Nombre de personnes en Good mood ************* \n \n")
        //df.filter(df("emotion") === "excited" || df("emotion") === "delighted" || df("emotion") === "pleased" || df("emotion") === "calm" || df("emotion")==="relax" || df("emotion")==="content").count()
    }

    def alertEmotions(df : DataFrame): DataFrame ={
        // position of people who are angry,afraid,alarmed, depressed
        print("\n \n ************* Position des personnes qui sont en Bad mood ************* \n \n")
        df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").agg(collect_list("lat").alias("lat"), collect_list("lon").alias("lon"))
        // show names of people who are angry,afraid,alarmed, depressed
        //print("\n \n ************* Noms des personnes qui sont en Bad mood ************* \n \n")
        //df.filter(df("emotion") === "angry" || df("emotion") === "afraid" || df("emotion") === "alarmed" || df("emotion") === "depressed").groupBy("name").agg(collect_list("words").alias("words")).show()
    }

    def nbWatcedCitizens(df : DataFrame): DataFrame ={
        // Show the number of people watched by a peacewatcher
        print("\n \n ************* Nombre peacewatched by peacewatcher  ************* \n \n")
        df.groupBy("id").count()
    }

    def mostReportedCitizens(df : DataFrame): DataFrame ={
        // show the peacewatcher with the highest count peacewatched
        print("\n \n ************* ID ************* \n \n")
        df.groupBy("id").count().sort(desc("count"))
    }
}