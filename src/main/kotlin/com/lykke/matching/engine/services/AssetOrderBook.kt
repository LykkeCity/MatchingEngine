package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.OrderBookEntry
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

open class AssetOrderBook(assetId: String) : AbstractAssetOrderBook(assetId) {
    companion object {
        private val SELL_COMPARATOR = Comparator<OrderBookEntry> { o1, o2 ->
            var result = o1.getOrderPrice().compareTo(o2.getOrderPrice())
            if (result == 0) {
                result = o1.getCreationDate().compareTo(o2.getCreationDate())
            }

            result
        }

        private val BUY_COMPARATOR = Comparator<OrderBookEntry> { o1, o2 ->
            var result = o2.getOrderPrice().compareTo(o1.getOrderPrice())
            if (result == 0) {
                result = o1.getCreationDate().compareTo(o2.getCreationDate())
            }

            result
        }

        fun <T: OrderBookEntry> sort(isBuySide: Boolean, orders: Array<T>): Array<T> {
            Arrays.sort(orders, if (isBuySide) BUY_COMPARATOR else SELL_COMPARATOR)
            return orders
        }
    }

    private var askOrderBook = PriorityBlockingQueue<LimitOrder>(50, SELL_COMPARATOR)
    private var bidOrderBook = PriorityBlockingQueue<LimitOrder>(50, BUY_COMPARATOR)

    override fun getOrderBook(isBuySide: Boolean) = if (isBuySide) bidOrderBook else askOrderBook

    fun setOrderBook(isBuySide: Boolean, queue: PriorityBlockingQueue<LimitOrder>) = if (isBuySide) bidOrderBook = queue else askOrderBook = queue

    override fun addOrder(order: LimitOrder) = getOrderBook(order.isBuySide()).add(order)

    override fun removeOrder(order: LimitOrder) = getOrderBook(order.isBuySide()).remove(order)

    fun getAskPrice() = askOrderBook.peek()?.price ?: BigDecimal.ZERO
    fun getBidPrice() = bidOrderBook.peek()?.price ?: BigDecimal.ZERO
    open fun getMidPrice(): BigDecimal? {
        return if (!NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, getAskPrice()) && !NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, getBidPrice())) {
            NumberUtils.divideWithMaxScale(getAskPrice() + getBidPrice(), BigDecimal.valueOf(2))
        } else
            null
    }

    fun getBestPrice(isBuy: Boolean): BigDecimal {
        return if (isBuy) getBidPrice() else getAskPrice()
    }

    fun leadToNegativeSpread(order: LimitOrder): Boolean {
        val book = getOrderBook(!order.isBuySide())
        if (book.isEmpty()) {
            return false
        }

        val bestPrice = book.peek().price
        if (order.isBuySide()) {
            return order.price >= bestPrice
        } else {
            return order.price <= bestPrice
        }
    }

    override fun copy(): AssetOrderBook {
        val book = AssetOrderBook(assetPairId)

        askOrderBook.forEach {
            book.askOrderBook.put(it)
        }

        bidOrderBook.forEach {
            book.bidOrderBook.put(it)
        }

        return book
    }

}