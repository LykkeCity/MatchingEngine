package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class LimitOrderProcessorFactory(private val genericLimitOrderService: GenericLimitOrderService,
                                 private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                 private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                 private val orderBookQueue: BlockingQueue<OrderBook>,
                                 private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                                 private val assetsHolder: AssetsHolder,
                                 private val assetsPairsHolder: AssetsPairsHolder,
                                 private val balancesHolder: BalancesHolder,
                                 private val applicationSettingsCache: ApplicationSettingsCache,
                                 private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>) {

    fun create(logger: Logger): LimitOrderProcessor {
        val matchingEngine = MatchingEngine(logger, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)
        val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, logger)
        return LimitOrderProcessor(genericLimitOrderService,
                trustedClientLimitOrderReportQueue,
                clientLimitOrderReportQueue,
                orderBookQueue,
                rabbitOrderBookQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache,
                lkkTradesQueue,
                matchingEngine,
                orderServiceHelper,
                logger)
    }
}