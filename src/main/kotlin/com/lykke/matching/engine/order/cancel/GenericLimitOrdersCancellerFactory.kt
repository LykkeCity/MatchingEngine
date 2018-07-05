package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrdersCancellerFactory(private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                         private val assetsPairsHolder: AssetsPairsHolder,
                                         private val balancesHolder: BalancesHolder,
                                         private val genericLimitOrderService: GenericLimitOrderService,
                                         private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                         private val genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                         private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                         private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                                         private val orderBookQueue: BlockingQueue<OrderBook>,
                                         private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                                         private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         private val messageSender: MessageSender) {

    fun create(logger: Logger, date: Date): GenericLimitOrdersCanceller {
        return GenericLimitOrdersCanceller(dictionariesDatabaseAccessor,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderService,
                genericStopLimitOrderService,
                genericLimitOrderProcessorFactory,
                trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue,
                orderBookQueue,
                rabbitOrderBookQueue,
                messageSequenceNumberHolder,
                messageSender,
                date,
                logger)
    }
}