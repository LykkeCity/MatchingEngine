package com.lykke.matching.engine.outgoing.messages.v2

import com.google.gson.GsonBuilder
import com.google.protobuf.GeneratedMessageV3
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import java.util.Date

abstract class AbstractEvent<out T : GeneratedMessageV3>(val header: Header): OutgoingMessage {

    companion object {
        private val gson = GsonBuilder()
                .registerTypeAdapter(Date::class.java, JsonSerializable.GmtDateTypeAdapter())
                .create()
    }

    fun sequenceNumber() = header.sequenceNumber

    abstract fun buildGeneratedMessage(): T

    override fun toString(): String {
        return gson.toJson(this)
    }

}