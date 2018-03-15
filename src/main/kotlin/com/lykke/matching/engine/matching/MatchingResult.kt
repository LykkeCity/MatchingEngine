package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.NewOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import java.util.Date
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

data class MatchingResult(
        /** Copy wrapper of incoming order */
        private val orderCopyWrapper: CopyWrapper<NewOrder>,
        val timestamp: Date,
        val cancelledLimitOrders: Set<NewLimitOrder> = HashSet(),
        /** Copy wrappers of matched orders */
        private val matchedOrders: List<CopyWrapper<NewLimitOrder>> = LinkedList(),
        val skipLimitOrders: Set<NewLimitOrder> = HashSet(),
        val completedLimitOrders: List<NewLimitOrder> = LinkedList(),
        val uncompletedLimitOrder: NewLimitOrder? = null,
        val lkkTrades: List<LkkTrade> = LinkedList(),
        val ownCashMovements: List<WalletOperation> = LinkedList(),
        val oppositeCashMovements: List<WalletOperation> = LinkedList(),
        val marketOrderTrades: List<TradeInfo> = LinkedList(),
        val limitOrdersReport: LimitOrdersReport? = null,
        val orderBook: PriorityBlockingQueue<NewLimitOrder> = PriorityBlockingQueue(),
        val marketBalance: Double? = null,
        private val autoApply: Boolean = true
) {

    val order: NewOrder = orderCopyWrapper.copy

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
