package com.peaceland.utils

import com.google.gson._
	
object MessageUtils {
	case class Message (
		id : String,
		user : String,
		text : String,
		place : String,
		country : String,
		lang : String
		)

	
	def parseFromJson(lines:Iterator[String]):Iterator[Message] = {
		val gson = new Gson
		lines.map(line => gson.fromJson(line, classOf[Message]))
	}
}
