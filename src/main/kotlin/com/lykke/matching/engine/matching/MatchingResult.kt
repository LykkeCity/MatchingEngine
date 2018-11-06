package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import java.math.BigDecimal
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

class MatchingResult(
        private val orderCopyWrapper: CopyWrapper<Order>,
        val cancelledLimitOrders: Set<CopyWrapper<LimitOrder>> = HashSet(),
        private val matchedOrders: List<CopyWrapper<LimitOrder>> = LinkedList(),
        val skipLimitOrders: Set<LimitOrder> = HashSet(),
        val completedLimitOrders: List<CopyWrapper<LimitOrder>> = LinkedList(),
        matchedUncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null,
        uncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null,
        val lkkTrades: List<LkkTrade> = LinkedList(),
        val ownCashMovements: MutableList<WalletOperation> = LinkedList(),
        val oppositeCashMovements: List<WalletOperation> = LinkedList(),
        val marketOrderTrades: List<TradeInfo> = LinkedList(),
        val limitOrdersReport: LimitOrdersReport? = null,
        val orderBook: PriorityBlockingQueue<LimitOrder> = PriorityBlockingQueue(),
        val marketBalance: BigDecimal? = null,
        val matchedWithZeroLatestTrade: Boolean = false,
        private val autoApply: Boolean = true
) {

    val orderCopy: Order = orderCopyWrapper.copy
    val uncompletedLimitOrderCopy: LimitOrder? = matchedUncompletedLimitOrderWrapper?.copy
    val uncompletedLimitOrder: LimitOrder? = uncompletedLimitOrderWrapper?.origin

    fun apply() {
        orderCopyWrapper.applyToOrigin()
        matchedOrders.forEach { it.applyToOrigin() }
    }

    init {
        if (this.autoApply) {
            apply()
        }
    }


}
