package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class GenericLimitOrdersCancellerFactory @Autowired constructor(private val executionContextFactory: ExecutionContextFactory,
                                                                private val stopOrderBookProcessor: StopOrderBookProcessor,
                                                                private val executionDataApplyService: ExecutionDataApplyService,
                                                                private val dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                                                private val assetsHolder: AssetsHolder,
                                                                private val assetsPairsHolder: AssetsPairsHolder,
                                                                private val balancesHolder: BalancesHolder,
                                                                private val genericLimitOrderService: GenericLimitOrderService,
                                                                private val genericStopLimitOrderService: GenericStopLimitOrderService) {


    fun create(logger: Logger, date: Date, cancelAll: Boolean = false): GenericLimitOrdersCanceller {
        return GenericLimitOrdersCanceller(executionContextFactory,
                stopOrderBookProcessor,
                executionDataApplyService,
                dictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderService,
                genericStopLimitOrderService,
                date,
                cancelAll,
                logger)
    }
}