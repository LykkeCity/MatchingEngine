package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.Date
import java.util.concurrent.BlockingQueue

@Component
class GenericLimitOrdersCancellerFactory @Autowired constructor(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                private val assetsPairsHolder: AssetsPairsHolder,
                                                                private val balancesHolder: BalancesHolder,
                                                                private val genericLimitOrderService: GenericLimitOrderService,
                                                                private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                                private val genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                                                private val orderBookQueue: BlockingQueue<OrderBook>,
                                                                private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                                                private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                                private val trustedClientsLimitOrderQueue: BlockingQueue<LimitOrdersReport>) {

    fun create(logger: Logger, date: Date): GenericLimitOrdersCanceller {
        return GenericLimitOrdersCanceller(dictionariesDatabaseAccessor,
                assetsPairsHolder,
                balancesHolder,
                orderBookQueue,
                rabbitOrderBookQueue,
                clientLimitOrdersQueue,
                trustedClientsLimitOrderQueue,
                genericLimitOrderService,
                genericStopLimitOrderService,
                genericLimitOrderProcessorFactory,
                date,
                logger)
    }
}