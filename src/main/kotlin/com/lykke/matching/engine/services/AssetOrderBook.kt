package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue

class AssetOrderBook(val assetId: String) {

    val SELL_COMPARATOR = Comparator<LimitOrder>({ o1, o2 ->
        var result = o1.price.compareTo(o2.price)
        if (result == 0) {
            result = o1.createdAt.compareTo(o2.createdAt)
        }

        result
    })

    val BUY_COMPARATOR = Comparator<LimitOrder>({ o1, o2 ->
        var result = o2.price.compareTo(o1.price)
        if (result == 0) {
            result = o1.createdAt.compareTo(o2.createdAt)
        }

        result
    })

    //TODO add time to comparators
    val askOrderBook = PriorityBlockingQueue<LimitOrder>(50, SELL_COMPARATOR)
    val bidOrderBook = PriorityBlockingQueue<LimitOrder>(50, BUY_COMPARATOR)

    fun getOrderBook(isBuySide: Boolean) = if (isBuySide) bidOrderBook else askOrderBook

    fun addOrder(order: LimitOrder) = getOrderBook(order.isBuySide()).add(order)

    fun removeOrder(order: LimitOrder) = getOrderBook(order.isBuySide()).remove(order)

    fun getAskPrice() = askOrderBook.peek()?.price ?: 0.0
    fun getBidPrice() = bidOrderBook.peek()?.price ?: 0.0
}