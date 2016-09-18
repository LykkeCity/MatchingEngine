package com.lykke.matching.engine.logging

import com.google.gson.Gson

class Error(val Type: String, val Message: String): LoggableObject {
    val Sender = "ME"

    override fun toString(): String{
        return "Error(Sender=$'$Sender', Type='$Type', Error='$Message')"
    }

    override fun getJson(): String {
        return Gson().toJson(this)
    }
}