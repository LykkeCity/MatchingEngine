package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue

@Component
class GenericLimitOrdersCancellerFactory @Autowired constructor(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                private val assetsHolder: AssetsHolder,
                                                                private val assetsPairsHolder: AssetsPairsHolder,
                                                                private val balancesHolder: BalancesHolder,
                                                                private val genericLimitOrderService: GenericLimitOrderService,
                                                                private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                                private val genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                                                private val orderBookQueue: BlockingQueue<OrderBook>,
                                                                private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                                                private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                                private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                                private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                                                private val messageSender: MessageSender,
                                                                private val midPriceHolder: MidPriceHolder) {


    fun create(logger: Logger,
               date: Date,
               cancelAll: Boolean = false): GenericLimitOrdersCanceller {
        return GenericLimitOrdersCanceller(dictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                orderBookQueue,
                rabbitOrderBookQueue,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                genericLimitOrderService,
                genericStopLimitOrderService,
                genericLimitOrderProcessorFactory,
                messageSequenceNumberHolder,
                messageSender,
                date,
                midPriceHolder,
                logger,
                cancelAll)
    }
}