package com.lykke.matching.engine.history

import com.microsoft.azure.storage.blob.CloudBlob
import java.io.ByteArrayOutputStream
import java.util.LinkedList
import java.util.StringJoiner


class TickBlobHolder {
    val name: String
    var askTicks = LinkedList<Double>()
    var bidTicks = LinkedList<Double>()

    constructor(name: String, blob: CloudBlob?) {
        this.name = name
        parseBlob(blob)
    }

    fun addTick(askPrice: Double, bidPrice: Double) {
        addTick(askPrice, askTicks)
        addTick(bidPrice, bidTicks)
    }

    fun addTick(price: Double, prices: LinkedList<Double>) {
        prices.add(price)
        while (prices.size < 4000) {
            prices.add(price)
        }
        if (prices.size > 4000) {
            prices.removeFirst()
        }
    }

    fun parseBlob(blob: CloudBlob?): LinkedList<Double> {
        val result = LinkedList<Double>()
        val data = getBlobValue(blob)
        if (data != null) {
            for (price in data.split(";")) {
                val prices = price.split(",")
                askTicks.add(prices[0].toDouble())
                bidTicks.add(prices[1].toDouble())
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
        for (i in 0..askTicks.size-1) {
            joiner.add("${askTicks[i]},${bidTicks[i]}")
        }
        return joiner.toString()
    }
}