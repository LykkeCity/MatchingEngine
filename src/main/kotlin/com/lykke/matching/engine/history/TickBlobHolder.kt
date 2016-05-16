package com.lykke.matching.engine.history

import com.microsoft.azure.storage.blob.CloudBlob
import java.io.ByteArrayOutputStream
import java.util.LinkedList
import java.util.StringJoiner


class TickBlobHolder {
    val name: String
    var ticks: LinkedList<Double>

    constructor(name: String, blob: CloudBlob?) {
        this.name = name
        ticks = parseBlob(blob)
    }

    fun addTick(tick: Double) {
        ticks.add(tick)
        while (ticks.size < 4000) {
            ticks.add(tick)
        }
        if (ticks.size > 4000) {
            ticks.removeFirst()
        }
    }

    fun parseBlob(blob: CloudBlob?): LinkedList<Double> {
        val result = LinkedList<Double>()
        val data = getBlobValue(blob)
        if (data != null) {
            for (price in data.split(";")) {
                result.add(price.toDouble())
            }
        }
        return result
    }

    fun getBlobValue(blob: CloudBlob?): String? {
        if (blob != null) {
            val outputStream = ByteArrayOutputStream()
            blob.download(outputStream)
            return outputStream.toString()
        }
        return null
    }

    fun convertToString(): String {
        val joiner = StringJoiner(";")
        for (tick in ticks) {
            joiner.add(tick.toString())
        }
        return joiner.toString()
    }
}