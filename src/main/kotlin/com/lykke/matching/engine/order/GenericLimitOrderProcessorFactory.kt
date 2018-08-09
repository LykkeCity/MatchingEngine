package com.lykke.matching.engine.order

import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class GenericLimitOrderProcessorFactory @Autowired constructor(private val genericLimitOrderService: GenericLimitOrderService,
                                                               private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                               private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                                                               private val limitOrderBusinessValidator: LimitOrderBusinessValidator,
                                                               private val assetsHolder: AssetsHolder,
                                                               private val assetsPairsHolder: AssetsPairsHolder,
                                                               private val balancesHolder: BalancesHolder,
                                                               private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                               private val feeProcessor: FeeProcessor,
                                                               private val singleLimitOrderParser: SingleLimitOrderContextParser,
                                                               private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                                               private val messageSender: MessageSender) {
    fun create(logger: Logger): GenericLimitOrderProcessor {
        return GenericLimitOrderProcessor(genericLimitOrderService,
                genericStopLimitOrderService,
                singleLimitOrderParser,
                clientLimitOrdersQueue,
                limitOrdersProcessorFactory,
                balancesHolder,
                limitOrderBusinessValidator,
                MatchingEngine(logger, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, feeProcessor),
                messageSequenceNumberHolder,
                messageSender,
                logger)
    }
}