package com.peaceland.producer;

import scala.io.Source
import scala.util.Random
import scala.math.BigDecimal
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.List;

import io.alphash.faker._
import play.api.libs.json._

object drone {

    private val words = Source.fromFile("resources/words.txt").getLines.toList
    private val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    implicit val citizenWrites = Json.writes[Citizen]
    implicit val reportWrites = Json.writes[Report]

    def main(args: Array[String]): Unit = {
        val report = jsonReport(1, pretty=true)
        println(report)
    }

    def randomCitizens(nb: Int): List[Citizen] = nb match {
        case x if x < 1 => Nil
        case _ => {
            val randomName = Person().name
            val randomScore = BigDecimal(Random.nextDouble)
                    .setScale(2, BigDecimal.RoundingMode.HALF_UP)
                    .toDouble

            Citizen(randomName, randomScore) :: randomCitizens(nb - 1)
        }
    }

    def randomWords(nb: Int): List[String] = nb match {
        case x if x < 1 => Nil
        case _ => this.words(Random.nextInt(this.words.length)) :: randomWords(nb - 1)
    }

    def randomCitizensWords(): (List[Citizen], List[String]) = {
        val nbCitizen = Random.nextInt(5)

        (randomCitizens(nbCitizen), randomWords(nbCitizen * 10))
    }

    def randomReport(id: Int): Report = {
        val dt = this.formatter.format(Calendar.getInstance().getTime())
        val lat = Geolocation().latitute
        val lon = Geolocation().longitude
        val citizenWords = randomCitizensWords()

        Report(id, dt, lat, lon, citizenWords._1, citizenWords._2)
    }

    def jsonReport(id: Int, pretty: Boolean = false): String = {
        if (pretty) {
            Json.prettyPrint(Json.toJson(randomReport(id)))
        } else {
            Json.stringify(Json.toJson(randomReport(id)))
        }
    }
}