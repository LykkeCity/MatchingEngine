package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import java.util.Date
import java.util.concurrent.BlockingQueue

class LimitOrdersCancellerFactory(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  private val assetsPairsHolder: AssetsPairsHolder,
                                  private val balancesHolder: BalancesHolder,
                                  private val genericLimitOrderService: GenericLimitOrderService,
                                  private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                  private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                  private val orderBookQueue: BlockingQueue<OrderBook>,
                                  private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>) {

    fun create(date: Date) = LimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsPairsHolder,
            balancesHolder,
            genericLimitOrderService,
            trustedClientsLimitOrdersQueue,
            clientsLimitOrdersQueue,
            orderBookQueue,
            rabbitOrderBookQueue,
            date)
}