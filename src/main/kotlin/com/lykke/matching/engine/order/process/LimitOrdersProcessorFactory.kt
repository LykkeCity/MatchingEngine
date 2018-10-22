package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

@Component
class LimitOrdersProcessorFactory(private val balancesHolder: BalancesHolder,
                                  private val singleLimitOrderBusinessValidator: LimitOrderBusinessValidator,
                                  private val limitOrderInputValidator: LimitOrderInputValidator,
                                  private val genericLimitOrderService: GenericLimitOrderService,
                                  private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                  private val orderBookQueue: BlockingQueue<OrderBook>,
                                  private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                  private val messageSender: MessageSender,
                                  private val applicationSettingsCache: ApplicationSettingsCache,
                                  private val midPriceHolder: MidPriceHolder) {

    fun create(matchingEngine: MatchingEngine,
               date: Date,
               isTrustedClient: Boolean,
               clientId: String,
               assetPair: AssetPair,
               baseAsset: Asset,
               quotingAsset: Asset,
               orderBook: AssetOrderBook,
               payBackBaseReserved: BigDecimal,
               payBackQuotingReserved: BigDecimal,
               ordersToCancel: Collection<LimitOrder>,
               clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
               trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
               LOGGER: Logger) =

            LimitOrdersProcessor(
                    isTrustedClient,
                    baseAsset,
                    quotingAsset,
                    limitOrderInputValidator,
                    balancesHolder,
                    genericLimitOrderService,
                    applicationSettingsCache,
                    midPriceHolder,
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
                    singleLimitOrderBusinessValidator,
                    messageSequenceNumberHolder,
                    messageSender,
                    LOGGER)
}