package com.lykke.matching.engine.history

import com.lykke.matching.engine.daos.TickUpdateInterval
import java.util.*


data class TickBlobHolder(val assetPair: String,
                     val tickUpdateInterval: TickUpdateInterval,
                     val askTicks: LinkedList<Double>,
                     val bidTicks: LinkedList<Double>,
                     val lastUpdate: Long,
                     val frequency: Long) {

    constructor(tickBlobHolder: TickBlobHolder): this(assetPair =  tickBlobHolder.assetPair,
            tickUpdateInterval = tickBlobHolder.tickUpdateInterval,
            askTicks = tickBlobHolder.askTicks.clone() as LinkedList<Double>,
            bidTicks = tickBlobHolder.bidTicks.clone() as LinkedList<Double>,
            lastUpdate = tickBlobHolder.lastUpdate,
            frequency = tickBlobHolder.frequency)

    constructor(assetPair: String,
                tickUpdateInterval: TickUpdateInterval,
                ask: Double,
                bid: Double,
                lastUpdate: Long,
                frequency: Long): this(assetPair =  assetPair,
            tickUpdateInterval = tickUpdateInterval,
            lastUpdate = lastUpdate,
            frequency = frequency) {
        addPrice(ask, bid)
    }

    constructor(assetPair: String,
                tickUpdateInterval: TickUpdateInterval,
                lastUpdate: Long,
                frequency: Long): this(assetPair =  assetPair,
            tickUpdateInterval = tickUpdateInterval,
            askTicks = LinkedList(),
            bidTicks = LinkedList(),
            lastUpdate = lastUpdate,
            frequency = frequency)

    companion object {
        val PRICE_PAIR_DELIMITER = ";"
    }

    fun addPrice(askPrice: Double, bidPrice: Double) {
        addPrice(askPrice, askTicks)
        addPrice(bidPrice, bidTicks)
    }

    private fun addPrice(price: Double, prices: LinkedList<Double>) {
        prices.add(price)
        while (prices.size < frequency) {
            prices.add(price)
        }
        if (prices.size > frequency) {
            prices.removeFirst()
        }
    }

    override fun toString(): String {
        val joiner = StringJoiner(PRICE_PAIR_DELIMITER)

        val askTicks = askTicks
        val bidTicks = bidTicks
        for (i in 0..askTicks.size-1) {
            joiner.add("${askTicks[i]},${bidTicks[i]}")
        }
        return joiner.toString()
    }
}