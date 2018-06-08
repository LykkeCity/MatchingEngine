package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.Date
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

data class MatchingResult(
        private val orderCopyWrapper: CopyWrapper<Order>,
        val timestamp: Date,
        val cancelledLimitOrders: Set<CopyWrapper<LimitOrder>> = HashSet(),
        private val matchedOrders: List<CopyWrapper<LimitOrder>> = LinkedList(),
        val skipLimitOrders: Set<LimitOrder> = HashSet(),
        val completedLimitOrders: List<CopyWrapper<LimitOrder>> = LinkedList(),
        private val matchedUncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null,
        private val uncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null,
        val lkkTrades: List<LkkTrade> = LinkedList(),
        val ownCashMovements: List<WalletOperation> = LinkedList(),
        val oppositeCashMovements: List<WalletOperation> = LinkedList(),
        val marketOrderTrades: List<TradeInfo> = LinkedList(),
        val limitOrdersReport: LimitOrdersReport? = null,
        val orderBook: PriorityBlockingQueue<LimitOrder> = PriorityBlockingQueue(),
        val marketBalance: BigDecimal? = null,
        private val autoApply: Boolean = true
) {

    val order: Order = orderCopyWrapper.copy
    val uncompletedLimitOrderCopy: LimitOrder? = matchedUncompletedLimitOrderWrapper?.copy
    val uncompletedLimitOrder: LimitOrder? = uncompletedLimitOrderWrapper?.origin

    fun apply() {
        orderCopyWrapper.applyToOrigin()
        matchedOrders.forEach { it.applyToOrigin() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MatchingResult

        if (orderCopyWrapper != other.orderCopyWrapper) return false
        if (timestamp != other.timestamp) return false
        if (cancelledLimitOrders != other.cancelledLimitOrders) return false
        if (matchedOrders != other.matchedOrders) return false
        if (skipLimitOrders != other.skipLimitOrders) return false
        if (completedLimitOrders != other.completedLimitOrders) return false
        if (uncompletedLimitOrderWrapper != other.uncompletedLimitOrderWrapper) return false
        if (lkkTrades != other.lkkTrades) return false
        if (ownCashMovements != other.ownCashMovements) return false
        if (oppositeCashMovements != other.oppositeCashMovements) return false
        if (marketOrderTrades != other.marketOrderTrades) return false
        if (limitOrdersReport != other.limitOrdersReport) return false
        if (orderBook != other.orderBook) return false
        if (!NumberUtils.equalsIgnoreScale(marketBalance, other.marketBalance)) return false
        if (autoApply != other.autoApply) return false
        if (order != other.order) return false
        if (uncompletedLimitOrderCopy != other.uncompletedLimitOrderCopy) return false
        if (uncompletedLimitOrder != other.uncompletedLimitOrder) return false

        return true
    }

    override fun hashCode(): Int {
        var result = orderCopyWrapper.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + cancelledLimitOrders.hashCode()
        result = 31 * result + matchedOrders.hashCode()
        result = 31 * result + skipLimitOrders.hashCode()
        result = 31 * result + completedLimitOrders.hashCode()
        result = 31 * result + (uncompletedLimitOrderWrapper?.hashCode() ?: 0)
        result = 31 * result + lkkTrades.hashCode()
        result = 31 * result + ownCashMovements.hashCode()
        result = 31 * result + oppositeCashMovements.hashCode()
        result = 31 * result + marketOrderTrades.hashCode()
        result = 31 * result + (limitOrdersReport?.hashCode() ?: 0)
        result = 31 * result + orderBook.hashCode()
        result = 31 * result + (marketBalance?.stripTrailingZeros()?.hashCode() ?: 0)
        result = 31 * result + autoApply.hashCode()
        result = 31 * result + order.hashCode()
        result = 31 * result + (uncompletedLimitOrderCopy?.hashCode() ?: 0)
        result = 31 * result + (uncompletedLimitOrder?.hashCode() ?: 0)
        return result
    }

    init {
        if (this.autoApply) {
            apply()
        }
    }


}
