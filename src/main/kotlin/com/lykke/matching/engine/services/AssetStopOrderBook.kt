package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import org.apache.log4j.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

class AssetStopOrderBook(private val assetPairId: String) {

    companion object {

        private val LOGGER = Logger.getLogger(AssetStopOrderBook::class.java.name)

        private val LOWER_SELL_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
            var result = o2.lowerLimitPrice!!.compareTo(o1.lowerLimitPrice!!)
            if (result == 0) {
                result = o1.lowerPrice!!.compareTo(o2.lowerPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        })

        private val UPPER_SELL_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
            var result = o1.upperLimitPrice!!.compareTo(o2.upperLimitPrice!!)
            if (result == 0) {
                result = o1.upperPrice!!.compareTo(o2.upperPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        })

        private val LOWER_BUY_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
            var result = o2.lowerLimitPrice!!.compareTo(o1.lowerLimitPrice!!)
            if (result == 0) {
                result = o2.lowerPrice!!.compareTo(o1.lowerPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        })

        private val UPPER_BUY_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
            var result = o1.upperLimitPrice!!.compareTo(o2.upperLimitPrice!!)
            if (result == 0) {
                result = o2.upperPrice!!.compareTo(o1.upperPrice!!)
                if (result == 0) {
                    result = o1.createdAt.compareTo(o2.createdAt)
                }
            }

            result
        })
    }

    private var lowerAskOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, LOWER_SELL_COMPARATOR)
    private var upperAskOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, UPPER_SELL_COMPARATOR)
    private var lowerBidOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, LOWER_BUY_COMPARATOR)
    private var upperBidOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, UPPER_BUY_COMPARATOR)

    private val askOrderBook = ConcurrentHashMap<String, NewLimitOrder>(50)
    private val bidOrderBook = ConcurrentHashMap<String, NewLimitOrder>(50)

    fun getOrderBook(isBuySide: Boolean) = (if (isBuySide) bidOrderBook else askOrderBook).values.toList()

    fun addOrder(order: NewLimitOrder) {
        if (order.assetPairId != assetPairId) {
            LOGGER.error("Unable to add order ${order.externalId} (order asset pair: ${order.assetPairId}, order book asset pair: $assetPairId)")
            return
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
    }

    fun removeOrder(order: NewLimitOrder) {
        if (order.assetPairId != assetPairId) {
            LOGGER.error("Unable to remove order ${order.externalId} (order asset pair: ${order.assetPairId}, order book asset pair: $assetPairId)")
            return
        }
        if (order.isBuySide()) {
            bidOrderBook.remove(order.externalId)
            if (order.lowerLimitPrice != null) {
                lowerBidOrderBook.remove(order)
            }
            if (order.upperLimitPrice != null) {
                upperBidOrderBook.remove(order)
            }
        } else {
            askOrderBook.remove(order.externalId)
            if (order.lowerLimitPrice != null) {
                lowerAskOrderBook.remove(order)
            }
            if (order.upperLimitPrice != null) {
                upperAskOrderBook.remove(order)
            }
        }
    }

    fun getOrder(price: Double, isBuySide: Boolean, isLowerSide: Boolean): NewLimitOrder? {
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