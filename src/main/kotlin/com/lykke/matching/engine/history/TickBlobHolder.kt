package com.lykke.matching.engine.history

import com.lykke.matching.engine.daos.TickUpdateInterval
import java.math.BigDecimal
import java.util.*


class TickBlobHolder(val assetPair: String,
                     val tickUpdateInterval: TickUpdateInterval,
                     val askTicks: LinkedList<BigDecimal>,
                     val bidTicks: LinkedList<BigDecimal>,
                     var lastUpdate: Long,
                     val frequency: Long) {

    constructor(tickBlobHolder: TickBlobHolder): this(assetPair =  tickBlobHolder.assetPair,
            tickUpdateInterval = tickBlobHolder.tickUpdateInterval,
            askTicks = tickBlobHolder.askTicks.clone() as LinkedList<BigDecimal>,
            bidTicks = tickBlobHolder.bidTicks.clone() as LinkedList<BigDecimal>,
            lastUpdate = tickBlobHolder.lastUpdate,
            frequency = tickBlobHolder.frequency)

    constructor(assetPair: String,
                tickUpdateInterval: TickUpdateInterval,
                ask: BigDecimal,
                bid: BigDecimal,
                lastUpdate: Long,
                frequency: Long): this(assetPair =  assetPair,
            tickUpdateInterval = tickUpdateInterval,
            lastUpdate = lastUpdate,
            frequency = frequency) {
        addPrice(ask, bid, lastUpdate)
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

    fun addPrice(askPrice: BigDecimal, bidPrice: BigDecimal, lastUpdateTime: Long) {
        addPrice(askPrice, askTicks)
        addPrice(bidPrice, bidTicks)
        lastUpdate = lastUpdateTime
    }

    private fun addPrice(price: BigDecimal, prices: LinkedList<BigDecimal>) {
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TickBlobHolder

        if (assetPair != other.assetPair) return false
        if (tickUpdateInterval != other.tickUpdateInterval) return false

        return true
    }

    override fun hashCode(): Int {
        var result = assetPair.hashCode()
        result = 31 * result + tickUpdateInterval.hashCode()
        return result
    }
}