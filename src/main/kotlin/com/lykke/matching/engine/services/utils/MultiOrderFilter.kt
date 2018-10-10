package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import kotlin.Boolean
import kotlin.Comparator
import kotlin.Int
import kotlin.collections.ArrayList
import kotlin.collections.forEach
import kotlin.collections.sortWith
import kotlin.plus
import kotlin.times

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

    fun checkAndAdd(order: LimitOrder) {
        orders.add(order)
        if (!isTrustedClient) {
            return
        }
        if (order.isBuySide()) {
            if (!notSortedBuySide) {
                if (prevBidPrice == null || order.price < prevBidPrice) {
                    prevBidPrice = order.price
                    val volume = NumberUtils.setScaleRoundUp(order.volume * order.price, quotingAssetAccuracy)
                    if (usedQuotingAssetVolume + volume > quotingAssetAvailableBalance) {
                        order.updateStatus(OrderStatus.NotEnoughFunds, date)
                        LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $quotingAssetAvailableBalance, used: $usedQuotingAssetVolume)")
                    } else {
                        usedQuotingAssetVolume += volume
                    }
                } else {
                    notSortedBuySide = true
                    LOGGER.debug("[${order.assetPairId}] Buy orders are not sorted by price")
                }
            }
        } else {
            if (!notSortedSellSide) {
                if (prevAskPrice == null || order.price > prevAskPrice) {
                    prevAskPrice = order.price
                    val volume = order.getAbsVolume()
                    if (usedBaseAssetVolume + volume > baseAssetAvailableBalance) {
                        order.updateStatus(OrderStatus.NotEnoughFunds, date)
                        LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $baseAssetAvailableBalance, used: $usedBaseAssetVolume)")
                    } else {
                        usedBaseAssetVolume += volume
                    }
                } else {
                    notSortedSellSide = true
                    LOGGER.debug("[${order.assetPairId}] Sell orders are not sorted by price")
                }
            }
        }
    }

    fun checkIfNotSorted() {
        if (!isTrustedClient || !notSortedSellSide && !notSortedBuySide) {
            return
        }
        val buyOrders = ArrayList<LimitOrder>()
        val sellOrders = ArrayList<LimitOrder>()
        orders.forEach { order ->
            if (order.isBuySide()) {
                buyOrders.add(order)
            } else {
                sellOrders.add(order)
            }
        }
        buyOrders.sortWith(Comparator { order1, order2 -> -order1.price.compareTo(order2.price) })
        sellOrders.sortWith(Comparator { order1, order2 -> order1.price.compareTo(order2.price) })
        usedQuotingAssetVolume = BigDecimal.ZERO
        buyOrders.forEach { order ->
            val volume = NumberUtils.setScaleRoundUp(order.volume * order.price, quotingAssetAccuracy)
            if (usedQuotingAssetVolume + volume > quotingAssetAvailableBalance) {
                order.updateStatus(OrderStatus.NotEnoughFunds, date)
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $quotingAssetAvailableBalance, used: $usedQuotingAssetVolume)")
            } else {
                usedQuotingAssetVolume += volume
            }
        }
        usedBaseAssetVolume = BigDecimal.ZERO
        sellOrders.forEach { order ->
            val volume = order.getAbsRemainingVolume()
            if (usedBaseAssetVolume + volume > baseAssetAvailableBalance) {
                order.updateStatus(OrderStatus.NotEnoughFunds, date)
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $baseAssetAvailableBalance, used: $usedBaseAssetVolume)")
            } else {
                usedBaseAssetVolume += volume
            }
        }
    }

    fun getResult() = orders
}