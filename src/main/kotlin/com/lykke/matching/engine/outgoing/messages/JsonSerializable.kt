package com.lykke.matching.engine.outgoing.messages

import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import com.google.gson.JsonSyntaxException
import java.lang.reflect.Type
import java.math.BigDecimal
import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone


open class JsonSerializable {
    fun toJson():String {
        return GsonBuilder().registerTypeAdapter(Date::class.java, GmtDateTypeAdapter()).create().toJson(this)
    }

    class BigDecimalTypeAdapter : JsonSerializer<BigDecimal>, JsonDeserializer<BigDecimal> {
        override fun serialize(src: BigDecimal?, typeOfSrc: Type?, context: JsonSerializationContext?): JsonElement {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun deserialize(json: JsonElement?, typeOfT: Type?, context: JsonDeserializationContext?): BigDecimal {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

    }

    class GmtDateTypeAdapter: JsonSerializer<Date>, JsonDeserializer<Date> {
        private val dateFormat: DateFormat

        init {
            dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.US)
            dateFormat.timeZone = TimeZone.getTimeZone("UTC")
        }

        @Synchronized
        override fun serialize(date: Date, type: Type,
                                             jsonSerializationContext: JsonSerializationContext): JsonElement {
            synchronized(dateFormat) {
                val dateFormatAsString = dateFormat.format(date)
                return JsonPrimitive(dateFormatAsString)
            }
        }

        @Synchronized
        override fun deserialize(jsonElement: JsonElement, type: Type,
                                               jsonDeserializationContext: JsonDeserializationContext): Date {
            try {
                synchronized(dateFormat) {
                    return dateFormat.parse(jsonElement.asString)
                }
            } catch (e: ParseException) {
                throw JsonSyntaxException(jsonElement.asString, e)
            }
        }
    }
}