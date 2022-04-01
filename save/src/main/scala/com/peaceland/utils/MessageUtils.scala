package com.peaceland.utils

import com.google.gson._
	
object MessageUtils {
	case class Message (
		name : String,
		lastname : String,
		emotion : String,
		behavior : String,
		date : String,
		place : String
		)

	
	def parseFromJson(lines:Iterator[String]):Iterator[Message] = {
		val gson = new Gson
		lines.map(line => gson.fromJson(line, classOf[Message]))
	}
}
