package com.lykke.matching.engine.config.spring

import com.google.gson.*
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Scope
import java.lang.reflect.Type
import java.math.BigDecimal
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*

@Configuration
open class JsonConfig {
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    open fun gson(): Gson {
        return GsonBuilder()
                .registerTypeAdapter(Date::class.java, GmtDateTypeAdapter())
                .registerTypeAdapter(BigDecimal::class.java, BigDecimalAdapter())
                .create()
    }
}

class BigDecimalAdapter : JsonSerializer<BigDecimal> {
    override fun serialize(src: BigDecimal, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
        return JsonPrimitive(src.stripTrailingZeros())
    }
}

class GmtDateTypeAdapter : JsonSerializer<Date>, JsonDeserializer<Date> {
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