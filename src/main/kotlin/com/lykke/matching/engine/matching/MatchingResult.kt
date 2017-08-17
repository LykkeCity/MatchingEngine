package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

data class MatchingResult(
        val order: Order,
        val cancelledLimitOrders: Set<LimitOrder> = HashSet(),
        val skipLimitOrders: Set<LimitOrder> = HashSet(),
        val completedLimitOrders: List<LimitOrder> = LinkedList(),
        var uncompletedLimitOrder: LimitOrder? = null,
        val lkkTrades: List<LkkTrade> = LinkedList(),
        val cashMovements: List<WalletOperation> = LinkedList(),
        val marketOrderTrades: List<TradeInfo> = LinkedList(),
        val limitOrdersReport: LimitOrdersReport? = null,
        val orderBook: PriorityBlockingQueue<LimitOrder> = PriorityBlockingQueue()
)