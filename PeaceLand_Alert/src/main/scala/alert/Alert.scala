package alert

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import plotly.Bar
import plotly.Plotly.TraceOps

//import scala.scalajs.js.annotation.JSExportTopLevel
import plotly.element.{Color, Marker, Orientation}
import plotly.layout.{BarMode, Layout}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.immutable._
import plotly._
import element._
import layout._
import Plotly._
import cats.Inject
import org.apache.hadoop.yarn.webapp.view.Html
//import org.scalajs.dom
//import org.scalajs.dom.window
//import org.scalajs.dom.window.{document, window}

import java.io._
import java.io.File
//import scala.scalajs.js.annotation.JSExportTopLevel

object Alert {

    /*def appendPar(targetNode: dom.Node, text: String): Unit = {
        val parNode = dom.document.createElement("p")
        parNode.textContent = text
        targetNode.appendChild(parNode)
    }*/

    /**
     * It streams the citizen report from drone to the analysis job
     *
     * @param args
     */
    def main(args: Array[String]): Unit = {
        loadCitizenLowPscore()
            /*document.addEventListener("DOMContentLoaded", { (e: dom.Event) =>
            setupUI()
        }) */
    }

    /*@JSExportTopLevel("addClickedMessage")
    def addClickedMessage(): Unit = {
        appendPar(document.body, "You clicked the button!")
    }

    def setupUI(): Unit = {
        val button = document.createElement("button")
        button.textContent = "Click me!"
        button.addEventListener("click", { (e: dom.MouseEvent) =>
            addClickedMessage()
        })
        dom.document.body.appendChild(button)

        appendPar(dom.document.body, "Hello World")
    }*/

    def loadCitizenLowPscore(): Unit ={
        val spark = SparkSession
          .builder
          .config("spark.master", "local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        // Creates a schema for citizenReport
        val schema = new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("age", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", IntegerType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", StringType)

        val threshold = 5

        // citizens with too low peace score (under 5/20)
        val suspiscious = spark.readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "test1")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()
          .selectExpr("CAST(value AS STRING)")
          .select(from_json($"value", schema).as("report"))
          .select("report.*")
          .filter($"pscore" < threshold)
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination()

        suspiscious
    }

    def retrieveAlerts(): Unit ={
        // Creates spark session using hdfs cluster
        val spark = SparkSession
          .builder
          .config("spark.master", "local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        // Creates a schema for citizenReport
        val schema = new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("age", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", IntegerType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", StringType)

        // citizens with too low peace score (under 5/20)
        val angry = spark.readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "test")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()
          .selectExpr("CAST(value AS STRING)")
          .select(from_json($"value", schema).as("report"))
          .select("report.*")
          .filter($"emotion" === "angry" )
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination()

        val sqlContext = spark.sqlContext
        val limit_alert = 5
        val limit_display = 5

        new File("/home/julie/Desktop/Data_Engineering_EKANE_NGAN_NZEKET/Website_Report/red_hours.html").delete()
        new File("/home/julie/Desktop/Data_Engineering_EKANE_NGAN_NZEKET/Website_Report/suspiscious_people.html").delete()

        val df = sqlContext.read.format("csv")
          .option("header", "false")
          .option("delimiter", ";")
          .schema(schema)
          .load("/home/julie/Desktop/Data_Engineering_EKANE_NGAN_NZEKET/PeaceLand_DataLake/data/data/*.csv")
          .withColumn("name", regexp_replace(col("name"), "\\[|\\]", ""))
          .withColumn("words", regexp_replace(col("words"), "\\[|\\]", ""))
          .withColumn("pscore", regexp_replace(col("pscore"), "\\[|\\]", ""))
          .withColumn("NamesArray", split(col("name"), ","))
          .withColumn("ScoresArray", split(col("pscore"), ",").cast("array<int>"))
          .withColumn("WordsArray", split(col("words"), ","))
          .drop("words")
          .drop("name")
          .drop("pscore")

        println("Most frequently used words during an alert :")
        val words_df = suspiscious_words(df, limit_alert).limit(limit_display)

        println("People most present during an alert :")
        val names_df = suspiscious_people(df, limit_alert).limit(limit_display)
        val names = names_df.select(col("names")).map(f => f.getString(0)).collect.toList
        val name_count = names_df.select(col("count")).map(f => f.getLong(0)).collect.toList

        println("Hours during which alerts occur the most :")
        val hours_df = alert_per_hour(df, limit_alert)

        val hours = hours_df.select(col("Hour")).map(f => f.getString(0)).collect.toList
        val hour_count = hours_df.select(col("count")).map(f => f.getLong(0)).collect.toList

        val name_plot = Bar(names, name_count)
        val hour_plot = Bar(hours, hour_count)
        val name_lay = Layout().withTitle(limit_display + " most present people during an alert")
        val hour_lay = Layout().withTitle("Hours during which alerts occur the most")

        name_plot.plot("/home/julie/Desktop/Data_Engineering_EKANE_NGAN_NZEKET/Website_Report/suspiscious_people.html", name_lay)
        hour_plot.plot("/home/julie/Desktop/Data_Engineering_EKANE_NGAN_NZEKET/Website_Report/red_hours.html", hour_lay)

        spark.stop()
    }

    def suspiscious_words(df: DataFrame, limit: Double): DataFrame = {
        val words = df.withColumn("critical_score", array_min(col("ScoresArray")))
          .filter(col("critical_score") < limit)
          .withColumn("words", explode(col("WordsArray")))
          .groupBy("words")
          .count()
          .sort(col("count").desc)

        words
    }

    def suspiscious_people(df: DataFrame, limit: Double): DataFrame = {
        val names = df.withColumn("critical_score", array_min(col("ScoresArray")))
          .filter(col("critical_score") < limit)
          .withColumn("names", explode(col("NamesArray")))
          .groupBy("names")
          .count()
          .sort(col("count").desc)
        names
    }

    def rate_alerts(df: DataFrame, limit: Double): (Long, Long) = {

        val nb_alerts = df.withColumn("critical_score", array_min(col("ScoresArray")))
          .filter(col("critical_score") < limit)
          .count()

        val weekend_alerts = df.withColumn("DayOfWeek", date_format(col("datetime"), "EEEE"))
          .withColumn("critical_score", array_min(col("ScoresArray")))
          .filter(col("critical_score") < limit)
          .filter(col("DayOfWeek") === "Saturday" || col("DayOfWeek") === "Sunday")
          .count()

        (nb_alerts, weekend_alerts)
    }

    def alert_per_hour(df: DataFrame, limit: Double): DataFrame = {

        val alert_per_hour = df.withColumn("critical_score", array_min(col("ScoresArray")))
          .filter(col("critical_score") < limit)
          .withColumn("Hour", date_format(col("datetime"), "H"))
          .groupBy("Hour")
          .count()
          .sort(col("count").desc)

        alert_per_hour
    }
}
