package com.lykke.matching.engine.queue.transaction

import com.google.gson.Gson

interface Transaction {
    fun toJson():String {
        return Gson().toJson(this)
    }
}