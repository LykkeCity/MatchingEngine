package com.lykke.matching.engine.outgoing.messages

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import com.google.gson.JsonSyntaxException
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import java.lang.reflect.Type
import java.math.BigDecimal
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone


open class JsonSerializable: OutgoingMessage {

    @JsonIgnore
    override fun isNewMessageFormat() = false

    companion object {
        private val gson = GsonBuilder()
                .registerTypeAdapter(Date::class.java, GmtDateTypeAdapter())
                .registerTypeAdapter(BigDecimal::class.java, BigDecimalAdapter())
                .create()
    }

    fun toJson():String {
        return gson.toJson(this)
    }

    class BigDecimalAdapter: JsonSerializer<BigDecimal> {
        override fun serialize(src: BigDecimal, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
            return JsonPrimitive(src.stripTrailingZeros())
        }
    }

    class GmtDateTypeAdapter: JsonSerializer<Date>, JsonDeserializer<Date> {
        override fun serialize(date: Date, type: Type,
                                             jsonSerializationContext: JsonSerializationContext): JsonElement {
            val dateFormat = tl.get()
            val dateFormatAsString = dateFormat.format(date)
            return JsonPrimitive(dateFormatAsString)
        }

        override fun deserialize(jsonElement: JsonElement, type: Type,
                                               jsonDeserializationContext: JsonDeserializationContext): Date {
            try {
                val dateFormat = tl.get()
                return dateFormat.parse(jsonElement.asString)
            } catch (e: ParseException) {
                throw JsonSyntaxException(jsonElement.asString, e)
            }
        }

        private val tl = object : ThreadLocal<SimpleDateFormat>() {
            override fun initialValue(): SimpleDateFormat {
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.US)
                sdf.timeZone = TimeZone.getTimeZone("UTC")
                return sdf
            }
        }
    }
}