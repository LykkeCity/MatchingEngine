package com.lykke.matching.engine.order

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class GenericLimitOrderProcessorFactory(private val genericLimitOrderService: GenericLimitOrderService,
                                        private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                        private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                                        private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                        private val assetsHolder: AssetsHolder,
                                        private val assetsPairsHolder: AssetsPairsHolder,
                                        private val balancesHolder: BalancesHolder,
                                        private val applicationSettingsCache: ApplicationSettingsCache) {

    fun create(logger: Logger): GenericLimitOrderProcessor {
        return GenericLimitOrderProcessor(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                clientLimitOrderReportQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache,
                MatchingEngine(logger, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder),
                logger)
    }
}