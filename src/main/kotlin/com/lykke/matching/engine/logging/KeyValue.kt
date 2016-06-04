package com.lykke.matching.engine.logging

import com.google.gson.Gson

class KeyValue(val Key: String, val Value: String): LoggableObject {
    override fun toString(): String{
        return "KeyValue(Key='$Key', Value='$Value')"
    }

    override fun getJson(): String {
        return Gson().toJson(arrayOf(this))
    }
}