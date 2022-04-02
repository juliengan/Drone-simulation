package stream

import com.google.gson.Gson

import scala.io.Source
import scala.util.Random
import scala.math.{BigDecimal}
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.List
import io.alphash.faker._
import play.api.libs.json._

object drone {
    case class ReportID(id : Int)
    case class Report (id : String, emotion : String, behavior : String, pscore : String, datetime: String, lat : String, lon: String, words : (Citizen, String))
    case class Citizen(name: String, score: Double)

    private val words = Source.fromFile("src/main/resources/words.txt").getLines.toList
    private val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    implicit val citizenWrites: OWrites[Citizen] = Json.writes[Citizen]
    implicit val messageWrites: OWrites[Report] = Json.writes[Report]

    def main(args: Array[String]): Unit = {
        val report = jsonReport(1, pretty=true)
        println(report)
    }

    def randomCitizens(nb: Int): Citizen = nb match {
        case x if x < 1 => ???
        case _ =>
            val randomName = Person().name
            val randomScore = BigDecimal(Random.nextDouble)
                    .setScale(2, BigDecimal.RoundingMode.HALF_UP)
                    .toDouble
            Citizen(randomName, randomScore)
    }


    // https://users.scala-lang.org/t/two-recursive-calls-one-tail-recursive-one-not/6166/12
    def randomWords(nb: Int): String = nb match {
        case x if x < 1 => ???
        case _ => this.words(Random.nextInt(this.words.length))
    }

    def randomCitizensWords(): (Citizen, String) = {
        val nbCitizen = Random.nextInt(5)
        (randomCitizens(nbCitizen), randomWords(nbCitizen * 10))
    }

    def randomReport(id:Integer): Report = {
        val datetime = this.formatter.format(Calendar.getInstance().getTime())
        val lat = Geolocation().latitute
        val lon = Geolocation().longitude
        val words = randomCitizensWords()
        val behavior = randomWords(1)
        val emotion = randomWords(1)
        val pscore = Random.nextInt(20)

        Report(id.toString, emotion, behavior, pscore.toString, datetime, lat.toString, lon.toString, words)
    }
    def parseFromJson(lines:Iterator[String]):Iterator[Report] = {
        val gson = new Gson
        lines.map(line => gson.fromJson(line, classOf[Report]))
    }

    def jsonReport(id: Int, pretty: Boolean = false): String = {
        if (pretty) {
            Json.prettyPrint(Json.toJson(randomReport(id)))
        } else {
            Json.stringify(Json.toJson(randomReport(id)))
        }
    }
}