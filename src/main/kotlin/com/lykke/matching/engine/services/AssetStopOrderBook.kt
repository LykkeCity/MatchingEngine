package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

class AssetStopOrderBook(assetPairId: String): AbstractAssetOrderBook(assetPairId) {
    companion object {

        private val LOGGER = Logger.getLogger(AssetStopOrderBook::class.java.name)

        private val LOWER_SELL_COMPARATOR = Comparator<LimitOrder> { o1, o2 ->
            var result = o2.lowerLimitPrice!!.compareTo(o1.lowerLimitPrice!!)
            if (result == 0) {
                result = o1.lowerPrice!!.compareTo(o2.lowerPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        }

        private val UPPER_SELL_COMPARATOR = Comparator<LimitOrder> { o1, o2 ->
            var result = o1.upperLimitPrice!!.compareTo(o2.upperLimitPrice!!)
            if (result == 0) {
                result = o1.upperPrice!!.compareTo(o2.upperPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        }

        private val LOWER_BUY_COMPARATOR = Comparator<LimitOrder> { o1, o2 ->
            var result = o2.lowerLimitPrice!!.compareTo(o1.lowerLimitPrice!!)
            if (result == 0) {
                result = o2.lowerPrice!!.compareTo(o1.lowerPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        }

        private val UPPER_BUY_COMPARATOR = Comparator<LimitOrder> { o1, o2 ->
            var result = o1.upperLimitPrice!!.compareTo(o2.upperLimitPrice!!)
            if (result == 0) {
                result = o2.upperPrice!!.compareTo(o1.upperPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        }
    }

    private var lowerAskOrderBook = PriorityBlockingQueue<LimitOrder>(50, LOWER_SELL_COMPARATOR)
    private var upperAskOrderBook = PriorityBlockingQueue<LimitOrder>(50, UPPER_SELL_COMPARATOR)
    private var lowerBidOrderBook = PriorityBlockingQueue<LimitOrder>(50, LOWER_BUY_COMPARATOR)
    private var upperBidOrderBook = PriorityBlockingQueue<LimitOrder>(50, UPPER_BUY_COMPARATOR)

    private val askOrderBook = ConcurrentHashMap<String, LimitOrder>(50)
    private val bidOrderBook = ConcurrentHashMap<String, LimitOrder>(50)

    override fun getOrderBook(isBuySide: Boolean) = (if (isBuySide) bidOrderBook else askOrderBook).values.toList()

    fun getOrderBook(isBuySide: Boolean, isLower: Boolean) = if (isBuySide) {
        if (isLower) lowerBidOrderBook else upperBidOrderBook
    } else {
        if (isLower) lowerAskOrderBook else upperAskOrderBook
    }

    override fun addOrder(order: LimitOrder): Boolean {
        if (order.assetPairId != assetPairId) {
            LOGGER.error("Unable to add order ${order.externalId} (order asset pair: ${order.assetPairId}, order book asset pair: $assetPairId)")
            return false
        }
        if (order.isBuySide()) {
            bidOrderBook[order.externalId] = order
            if (order.lowerLimitPrice != null) {
                lowerBidOrderBook.add(order)
            }
            if (order.upperLimitPrice != null) {
                upperBidOrderBook.add(order)
            }
        } else {
            askOrderBook[order.externalId] = order
            if (order.lowerLimitPrice != null) {
                lowerAskOrderBook.add(order)
            }
            if (order.upperLimitPrice != null) {
                upperAskOrderBook.add(order)
            }
        }
        return true
    }

    override fun removeOrder(order: LimitOrder): Boolean {
        if (order.assetPairId != assetPairId) {
            LOGGER.error("Unable to remove order ${order.externalId} (order asset pair: ${order.assetPairId}, order book asset pair: $assetPairId)")
            return false
        }
        var result = false
        if (order.isBuySide()) {
            bidOrderBook.remove(order.externalId)
            if (order.lowerLimitPrice != null) {
                result = lowerBidOrderBook.remove(order) || result
            }
            if (order.upperLimitPrice != null) {
                result = upperBidOrderBook.remove(order) || result
            }
        } else {
            askOrderBook.remove(order.externalId)
            if (order.lowerLimitPrice != null) {
                result = lowerAskOrderBook.remove(order) || result
            }
            if (order.upperLimitPrice != null) {
                result = upperAskOrderBook.remove(order) || result
            }
        }
        return result
    }

    override fun copy(): AssetStopOrderBook {
        val book = AssetStopOrderBook(assetPairId)
        lowerAskOrderBook.forEach {
            book.lowerAskOrderBook.put(it)
        }
        upperAskOrderBook.forEach {
            book.upperAskOrderBook.put(it)
        }
        lowerBidOrderBook.forEach {
            book.lowerBidOrderBook.put(it)
        }
        upperBidOrderBook.forEach {
            book.upperBidOrderBook.put(it)
        }
        book.askOrderBook.putAll(askOrderBook)
        book.bidOrderBook.putAll(bidOrderBook)
        return book
    }

    fun getOrder(price: BigDecimal, isBuySide: Boolean, isLowerSide: Boolean): LimitOrder? {
        val order = (if (isBuySide) {
            if (isLowerSide) lowerBidOrderBook else upperBidOrderBook
        } else {
            if (isLowerSide) lowerAskOrderBook else upperAskOrderBook
        }).peek() ?: return null

        return if (isLowerSide) {
            if (order.lowerLimitPrice!! >= price) order else null
        } else {
            if (order.upperLimitPrice!! <= price) order else null
        }
    }
}