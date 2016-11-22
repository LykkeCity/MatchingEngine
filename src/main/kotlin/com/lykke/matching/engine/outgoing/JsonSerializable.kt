package com.lykke.matching.engine.outgoing

import com.google.gson.Gson

open class JsonSerializable {
    fun toJson():String {
        return Gson().toJson(this)
    }
}