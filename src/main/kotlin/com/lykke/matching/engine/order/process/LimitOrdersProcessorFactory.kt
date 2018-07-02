package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

@Component
class LimitOrdersProcessorFactory(private val assetsHolder: AssetsHolder,
                                  private val assetsPairsHolder: AssetsPairsHolder,
                                  private val balancesHolder: BalancesHolder,
                                  private val genericLimitOrderService: GenericLimitOrderService,
                                  private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                  private val orderBookQueue: BlockingQueue<OrderBook>,
                                  private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  private val applicationSettingsCache: ApplicationSettingsCache) {

    fun create(matchingEngine: MatchingEngine,
               date: Date,
               clientId: String,
               assetPair: AssetPair,
               orderBook: AssetOrderBook,
               payBackBaseReserved: BigDecimal,
               payBackQuotingReserved: BigDecimal,
               ordersToCancel: Collection<LimitOrder>,
               clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
               trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
               LOGGER: Logger) =

            LimitOrdersProcessor(assetsHolder,
                    assetsPairsHolder,
                    balancesHolder,
                    genericLimitOrderService,
                    applicationSettingsCache,
                    ordersToCancel,
                    clientLimitOrdersQueue,
                    lkkTradesQueue,
                    orderBookQueue,
                    rabbitOrderBookQueue,
                    trustedClientsLimitOrdersQueue,
                    matchingEngine,
                    date,
                    clientId,
                    assetPair,
                    orderBook,
                    payBackBaseReserved,
                    payBackQuotingReserved,
                    clientsLimitOrdersWithTrades,
                    trustedClientsLimitOrdersWithTrades,
                    LOGGER)
}