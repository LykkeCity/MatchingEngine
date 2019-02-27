package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.OrderBookEntry
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
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

    fun leadToNegativeSpreadForClient(order: LimitOrder): Boolean {
        val book = getCopyOfOrderBook(!order.isBuySide())
        if (book.isEmpty()) {
            return false
        }
        while (book.isNotEmpty()) {
            val bookOrder = book.poll()
            if (if (order.isBuySide()) order.price >= bookOrder.price else order.price <= bookOrder.price) {
                if (bookOrder.clientId == order.clientId) return true
            } else {
                return false
            }
        }

        return false
    }

    fun leadToNegativeSpreadByOtherClient(order: LimitOrder): Boolean {
        val book = getCopyOfOrderBook(!order.isBuySide())
        if (book.isEmpty()) {
            return false
        }
        while (book.isNotEmpty()) {
            val bookOrder = book.poll()
            if (if (order.isBuySide()) order.price >= bookOrder.price else order.price <= bookOrder.price) {
                if (bookOrder.clientId != order.clientId) return true
            } else {
                return false
            }
        }

        return false
    }

    override fun copy() : AssetOrderBook {
        val book = AssetOrderBook(assetPairId)

        askOrderBook.forEach {
            book.askOrderBook.put(it)
        }

        bidOrderBook.forEach {
            book.bidOrderBook.put(it)
        }

        return book
    }

    fun getCopyOfOrderBook(isBuySide: Boolean) = if (isBuySide) copyQueue(bidOrderBook) else copyQueue(askOrderBook)

    fun copyQueue(queue: PriorityBlockingQueue<LimitOrder>): PriorityBlockingQueue<LimitOrder> {
        return PriorityBlockingQueue(queue)
    }
}