package com.lykke.matching.engine.outgoing.messages.v2.events

import com.google.gson.GsonBuilder
import com.google.protobuf.GeneratedMessageV3
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.OutgoingMessage
import java.util.Date

abstract class Event<out T : GeneratedMessageV3>(val header: Header): OutgoingMessage {

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