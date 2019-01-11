package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date

class MultiOrderFilter(private val isTrustedClient: Boolean,
                       private val baseAssetAvailableBalance: BigDecimal,
                       private val quotingAssetAvailableBalance: BigDecimal,
                       private val quotingAssetAccuracy: Int,
                       private val date: Date,
                       initialCapacity: Int,
                       private val LOGGER: Logger) {

    private var notSortedBuySide = false
    private var notSortedSellSide = false
    private var prevBidPrice: BigDecimal? = null
    private var prevAskPrice: BigDecimal? = null
    private var usedBaseAssetVolume = BigDecimal.ZERO
    private var usedQuotingAssetVolume = BigDecimal.ZERO
    private val orders = ArrayList<LimitOrder>(initialCapacity)
    private val rejectedOrders = mutableListOf<RejectedOrder>()
    private var done = false

    fun checkAndAdd(order: LimitOrder) {
        if (done) {
            throw IllegalStateException("Result is already returned")
        }
        orders.add(order)
        if (!isTrustedClient) {
            return
        }
        if (order.isBuySide()) {
            checkBuyOrder(order)
        } else {
            checkSellOrder(order)
        }
    }

    fun getResult(): List<LimitOrder> {
        if (!done) {
            done = true
            checkIfNotSorted()
        }
        return orders
    }

    private fun checkBuyOrder(order: LimitOrder) {
        if (notSortedBuySide) {
            return
        }
        if (prevBidPrice == null || order.price < prevBidPrice) {
            prevBidPrice = order.price
            checkQuotingAssetVolume(order)
            return
        }
        notSortedBuySide = true
        LOGGER.debug("[${order.assetPairId}] Buy orders are not sorted by price")
    }

    private fun checkQuotingAssetVolume(order: LimitOrder) {
        val quotingAssetVolume = NumberUtils.setScaleRoundUp(order.volume * order.price, quotingAssetAccuracy)
        if (usedQuotingAssetVolume + quotingAssetVolume <= quotingAssetAvailableBalance) {
            usedQuotingAssetVolume += quotingAssetVolume
            return
        }
        rejectedOrders.add(RejectedOrder(order, OrderStatus.valueOf(order.status)))
        order.updateStatus(OrderStatus.NotEnoughFunds, date)
        LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $quotingAssetAvailableBalance, used: $usedQuotingAssetVolume)")
    }

    private fun checkSellOrder(order: LimitOrder) {
        if (notSortedSellSide) {
            return
        }
        if (prevAskPrice == null || order.price > prevAskPrice) {
            prevAskPrice = order.price
            checkBaseAssetVolume(order)
            return
        }
        notSortedSellSide = true
        LOGGER.debug("[${order.assetPairId}] Sell orders are not sorted by price")
    }

    private fun checkBaseAssetVolume(order: LimitOrder) {
        val volume = order.getAbsVolume()
        if (usedBaseAssetVolume + volume <= baseAssetAvailableBalance) {
            usedBaseAssetVolume += volume
            return
        }
        rejectedOrders.add(RejectedOrder(order, OrderStatus.valueOf(order.status)))
        order.updateStatus(OrderStatus.NotEnoughFunds, date)
        LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $baseAssetAvailableBalance, used: $usedBaseAssetVolume)")
    }

    private fun checkIfNotSorted() {
        if (!isTrustedClient || !notSortedSellSide && !notSortedBuySide) {
            return
        }
        rollbackRejectionResult()
        val buyOrders = ArrayList<LimitOrder>()
        val sellOrders = ArrayList<LimitOrder>()
        orders.forEach { order ->
            if (order.isBuySide()) {
                buyOrders.add(order)
            } else {
                sellOrders.add(order)
            }
        }
        buyOrders.sortByDescending { it.price }
        sellOrders.sortBy { it.price }
        usedQuotingAssetVolume = BigDecimal.ZERO
        buyOrders.forEach { order ->
            checkQuotingAssetVolume(order)
        }
        usedBaseAssetVolume = BigDecimal.ZERO
        sellOrders.forEach { order ->
            checkBaseAssetVolume(order)
        }
    }

    private fun rollbackRejectionResult() {
        rejectedOrders.forEach { it.order.updateStatus(it.previousStatus, date) }
        rejectedOrders.clear()
        LOGGER.info("Rolled back rejection result")
    }
}

private class RejectedOrder(val order: LimitOrder,
                            val previousStatus: OrderStatus)