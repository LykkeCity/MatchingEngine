package com.lykke.matching.engine.logging

import com.google.gson.Gson
import java.util.Arrays

class Line(val Id: String, var Data: Array<KeyValue>) : LoggableObject {

    fun addKeyValue(keyValue: KeyValue) {
        Data.plus(keyValue)
    }

    override fun toString(): String{
        return "Line(Id='$Id', Data=${Arrays.toString(Data)})"
    }

    override fun getJson(): String {
        return Gson().toJson(this)
    }
}