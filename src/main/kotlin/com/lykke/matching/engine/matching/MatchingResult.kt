package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.NewOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

data class MatchingResult(
        val order: NewOrder,
        val cancelledLimitOrders: Set<NewLimitOrder> = HashSet(),
        val skipLimitOrders: Set<NewLimitOrder> = HashSet(),
        val completedLimitOrders: List<NewLimitOrder> = LinkedList(),
        var uncompletedLimitOrder: NewLimitOrder? = null,
        val lkkTrades: List<LkkTrade> = LinkedList(),
        val cashMovements: List<WalletOperation> = LinkedList(),
        val marketOrderTrades: List<TradeInfo> = LinkedList(),
        val limitOrdersReport: LimitOrdersReport? = null,
        val orderBook: PriorityBlockingQueue<NewLimitOrder> = PriorityBlockingQueue()
)