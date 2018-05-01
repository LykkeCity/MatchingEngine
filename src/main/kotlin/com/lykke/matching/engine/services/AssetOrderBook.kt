package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue

class AssetOrderBook(val assetId: String): AbstractAssetOrderBook {

    val SELL_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
        var result = o1.price.compareTo(o2.price)
        if (result == 0) {
            result = o1.createdAt.compareTo(o2.createdAt)
        }

        result
    })

    val BUY_COMPARATOR = Comparator<NewLimitOrder>({ o1, o2 ->
        var result = o2.price.compareTo(o1.price)
        if (result == 0) {
            result = o1.createdAt.compareTo(o2.createdAt)
        }

        result
    })

    private var askOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, SELL_COMPARATOR)
    private var bidOrderBook = PriorityBlockingQueue<NewLimitOrder>(50, BUY_COMPARATOR)

    fun getOrderBook(isBuySide: Boolean) = if (isBuySide) bidOrderBook else askOrderBook

    fun setOrderBook(isBuySide: Boolean, queue: PriorityBlockingQueue<NewLimitOrder>) = if (isBuySide) bidOrderBook = queue else askOrderBook = queue

    fun addOrder(order: NewLimitOrder) = getOrderBook(order.isBuySide()).add(order)

    override fun removeOrder(order: NewLimitOrder) = getOrderBook(order.isBuySide()).remove(order)

    fun getAskPrice() = askOrderBook.peek()?.price ?: 0.0
    fun getBidPrice() = bidOrderBook.peek()?.price ?: 0.0

    fun leadToNegativeSpread(order: NewLimitOrder): Boolean {
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

    fun leadToNegativeSpreadForClient(order: NewLimitOrder): Boolean {
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

    fun leadToNegativeSpreadByOtherClient(order: NewLimitOrder): Boolean {
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
        val book = AssetOrderBook(assetId)

        askOrderBook.forEach {
            book.askOrderBook.put(it)
        }

        bidOrderBook.forEach {
            book.bidOrderBook.put(it)
        }

        return book
    }

    fun getCopyOfOrderBook(isBuySide: Boolean) = if (isBuySide) copyQueue(bidOrderBook) else copyQueue(askOrderBook)

    fun copyQueue(queue: PriorityBlockingQueue<NewLimitOrder>): PriorityBlockingQueue<NewLimitOrder> {
        return PriorityBlockingQueue(queue)
    }
}