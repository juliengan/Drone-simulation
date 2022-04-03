package producer

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

    case class Message (id : String, name : String, age : String, emotion : String, behavior : String, pscore : String, datetime: String, lat : String, lon: String, words : String)
    private val words = Source.fromFile("src/main/scala/data/words.txt").getLines.toList
    private val emotions_bag = Source.fromFile("src/main/scala/data/emotions.txt").getLines.toList
    private val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    implicit val messageWrites: OWrites[Message] = Json.writes[Message]

    def main(args: Array[String]): Unit = {
        val report = jsonReport(1, pretty=true)
        println(report)
    }

    // https://users.scala-lang.org/t/two-recursive-calls-one-tail-recursive-one-not/6166/12
    def randomWords(nb: Int): String = nb match {
        case x if x < 1 => ???
        case _ => this.words(Random.nextInt(this.words.length))
    }

    def randomEmotions(nb: Int): String = nb match {
        case x if x < 1 => ???
        case _ => this.emotions_bag(Random.nextInt(this.emotions_bag.length))
    }

    def randomCitizens(nb: Int): String = nb match {
        case x if x < 1 => ???
        case _ => Person().name
    }

    def randomCitizensWords(): String = {
        val nbCitizen = Random.nextInt(5)
        randomWords(nbCitizen * 10)
    }

    def randomReport(id:Integer): Message = {
        val datetime = this.formatter.format(Calendar.getInstance().getTime())
        val lat = Geolocation().latitute
        val lon = Geolocation().longitude
        val words = randomCitizensWords()
        val behavior = randomWords(1)
        val emotion = randomEmotions(1)
        val pscore = Random.nextInt(20)
        val age = 5 + Random. nextInt(85)
        val name = randomCitizens(1)

        Message(id.toString, name, age.toString, emotion, behavior, pscore.toString, datetime, lat.toString, lon.toString, words)
    }

    def parseFromJson(lines:Iterator[String]):Iterator[Message] = {
        val gson = new Gson
        lines.map(line => gson.fromJson(line, classOf[Message]))
    }

    
    def jsonReport(id: Int, pretty: Boolean = false): String = {
        if (pretty) {
            Json.prettyPrint(Json.toJson(randomReport(id)))
        } else {
            Json.stringify(Json.toJson(randomReport(id)))
        }
    }
}

