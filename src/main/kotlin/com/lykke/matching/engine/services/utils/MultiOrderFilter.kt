package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import org.apache.log4j.Logger
import java.math.BigDecimal

class MultiOrderFilter(private val isTrustedClient: Boolean,
                       private val baseAssetAvailableBalance: BigDecimal,
                       private val quotingAssetAvailableBalance: BigDecimal,
                       private val orders: MutableCollection<LimitOrder>,
                       private val LOGGER: Logger) {

    private var needToBidSort = false
    private var needToAskSort = false
    private var prevBidPrice: BigDecimal? = null
    private var prevAskPrice: BigDecimal? = null
    private var usedBaseAssetVolume = BigDecimal.ZERO
    private var usedQuotingAssetVolume = BigDecimal.ZERO

    fun checkAndAdd(order: LimitOrder): Boolean {
        if (!isTrustedClient) {
            orders.add(order)
            return true
        }
        var skip = false
        if (order.isBuySide()) {
            if (!needToBidSort) {
                if (prevBidPrice == null || order.price < prevBidPrice) {
                    prevBidPrice = order.price
                    val volume = order.volume * order.price
                    if (usedQuotingAssetVolume + volume > quotingAssetAvailableBalance) {
                        skip = true
                        LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} (${order.externalId}) due to low balance (available: $quotingAssetAvailableBalance, used: $usedQuotingAssetVolume)")
                    } else {
                        usedQuotingAssetVolume += volume
                    }
                } else {
                    needToBidSort = true
                    LOGGER.debug("[${order.assetPairId}] Buy orders are not sorted by price")
                }
            }
        } else {
            if (!needToAskSort) {
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
                    needToAskSort = true
                    LOGGER.debug("[${order.assetPairId}] Sell orders are not sorted by price")
                }
            }
        }

        return if (!skip) {
            orders.add(order)
            true
        } else false
    }

    fun filterOutIfNotSorted(): List<LimitOrder> {
        if (!isTrustedClient || !needToAskSort && !needToBidSort) {
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
            val volume = order.volume * order.price
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
        usedBaseAssetVolume = BigDecimal.ZERO
        return ordersToReject
    }
}