package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal

class MultiOrderFilter(private val isTrustedClient: Boolean,
                       private val baseAssetAvailableBalance: BigDecimal,
                       private val quotingAssetAvailableBalance: BigDecimal,
                       private val quotingAssetAccuracy: Int,
                       initialCapacity: Int,
                       private val LOGGER: Logger) {

    private var notSortedBuySide = false
    private var notSortedSellSide = false
    private var prevBidPrice: BigDecimal? = null
    private var prevAskPrice: BigDecimal? = null
    private var usedBaseAssetVolume = BigDecimal.ZERO
    private var usedQuotingAssetVolume = BigDecimal.ZERO
    private val orders = ArrayList<LimitOrder>(initialCapacity)

    fun checkAndAdd(order: LimitOrder): Boolean {
        if (!isTrustedClient) {
            return orders.add(order)
        }
        var skip = false
        if (order.isBuySide()) {
            if (!notSortedBuySide) {
                if (prevBidPrice == null || order.price < prevBidPrice) {
                    prevBidPrice = order.price
                    val volume = NumberUtils.setScaleRoundUp(order.volume * order.price, quotingAssetAccuracy)
                    if (usedQuotingAssetVolume + volume > quotingAssetAvailableBalance) {
                        skip = true
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
                        skip = true
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

        return if (!skip) orders.add(order) else false
    }

    fun filterOutIfNotSorted(): List<LimitOrder> {
        if (!isTrustedClient || !notSortedSellSide && !notSortedBuySide) {
            return emptyList()
        }
        val buyOrders = ArrayList<LimitOrder>(orders.size)
        val sellOrders = ArrayList<LimitOrder>(orders.size)
        orders.forEach { order ->
            if (order.isBuySide()) {
                buyOrders.add(order)
            } else {
                sellOrders.add(order)
            }
        }
        buyOrders.sortWith(Comparator { order1, order2 -> -order1.price.compareTo(order2.price) })
        sellOrders.sortWith(Comparator { order1, order2 -> order1.price.compareTo(order2.price) })
        val ordersToReject = ArrayList<LimitOrder>(orders.size)
        usedQuotingAssetVolume = BigDecimal.ZERO
        ordersToReject.addAll(buyOrders.filter { order ->
            val volume = NumberUtils.setScaleRoundUp(order.volume * order.price, quotingAssetAccuracy)
            if (usedQuotingAssetVolume + volume > quotingAssetAvailableBalance) {
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $quotingAssetAvailableBalance, used: $usedQuotingAssetVolume)")
                true
            } else {
                usedQuotingAssetVolume += volume
                false
            }
        })
        usedBaseAssetVolume = BigDecimal.ZERO
        ordersToReject.addAll(sellOrders.filter { order ->
            val volume = order.getAbsRemainingVolume()
            if (usedBaseAssetVolume + volume > baseAssetAvailableBalance) {
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $baseAssetAvailableBalance, used: $usedBaseAssetVolume)")
                true
            } else {
                usedBaseAssetVolume += volume
                false
            }
        })
        orders.removeAll(ordersToReject)
        return ordersToReject
    }

    fun getResult() = orders
}