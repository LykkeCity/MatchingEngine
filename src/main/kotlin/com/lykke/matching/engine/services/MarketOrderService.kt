package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET_PAIR
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_MARKET_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.STATUS
import com.lykke.matching.engine.logging.STRAIGHT
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.ReservedVolumeGreaterThanBalance
import com.lykke.matching.engine.order.OrderStatus.UnknownAsset
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.ArrayList
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MarketOrderService(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                         private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                         private val genericLimitOrderService: GenericLimitOrderService,
                         assetsHolder: AssetsHolder,
                         private val assetsPairsHolder: AssetsPairsHolder,
                         private val balancesHolder: BalancesHolder,
                         private val trustedLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                         private val orderBookQueue: BlockingQueue<OrderBook>,
                         private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                         private val walletCredentialsCache: WalletCredentialsCache,
                         private val rabbitSwapQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.MarketOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0
    private var logCount = 1000
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        val order = if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            val message = parseOld(messageWrapper.byteArray)
            LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, straight: ${message.straight}")

            MarketOrder(UUID.randomUUID().toString(), message.uid.toString(), message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight, message.reservedLimitVolume)
        } else {
            val message = parse(messageWrapper.byteArray)
            LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, straight: ${message.straight}")

            MarketOrder(UUID.randomUUID().toString(), message.uid, message.assetPairId, message.clientId, message.volume, null,
                    Processing.name, Date(message.timestamp), Date(), null, message.straight, message.reservedLimitVolume)
        }

        try {
            assetsPairsHolder.getAssetPair(order.assetPairId)
        } catch (e: Exception) {
            order.status = UnknownAsset.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("Unknown asset: ${order.assetPairId}")
            writeResponse(messageWrapper, order, MessageStatus.UNKNOWN_ASSET, order.assetPairId)
            METRICS_LOGGER.log(getMetricLine(order.externalId, order))
            METRICS_LOGGER.log(KeyValue(ME_MARKET_ORDER, (++messagesCount).toString()))
            return
        }

        val orderBook = genericLimitOrderService.getOrderBook(order.assetPairId).getCopyOfOrderBook(!order.isBuySide())
        if (orderBook.size == 0) {
            order.status = NoLiquidity.name
            rabbitSwapQueue.put(MarketOrderWithTrades(order))
            LOGGER.info("No liquidity, no orders in order book, for market order id: ${order.id}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, straight: ${order.straight}")
            writeResponse(messageWrapper, order, MessageStatus.NO_LIQUIDITY)
            return
        }

        val matchingResult = matchingEngine.match(order, orderBook)
        val marketOrder = matchingResult.order as MarketOrder
        when (OrderStatus.valueOf(matchingResult.order.status)) {
            NoLiquidity -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(marketOrder))
                writeResponse(messageWrapper, marketOrder, MessageStatus.NO_LIQUIDITY)
            }
            ReservedVolumeGreaterThanBalance -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(marketOrder))
                writeResponse(messageWrapper, marketOrder, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, "Reserved volume is higher than available balance")
            }
            NotEnoughFunds -> {
                rabbitSwapQueue.put(MarketOrderWithTrades(marketOrder))
                writeResponse(messageWrapper, marketOrder, MessageStatus.NOT_ENOUGH_FUNDS)
            }
            Matched -> {
                genericLimitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)
                matchingResult.cancelledLimitOrders.forEach { limitOrder ->
                    limitOrder.status = OrderStatus.NotEnoughFunds.name
                }
                genericLimitOrderService.moveOrdersToDone(ArrayList<NewLimitOrder>(matchingResult.cancelledLimitOrders))

                matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                if (matchingResult.uncompletedLimitOrder != null) {
                    genericLimitOrderService.updateLimitOrder(matchingResult.uncompletedLimitOrder as NewLimitOrder)
                    matchingResult.orderBook.put(matchingResult.uncompletedLimitOrder)
                }

                genericLimitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)

                marketOrderDatabaseAccessor.addLkkTrades(matchingResult.lkkTrades)

                balancesHolder.processWalletOperations(order.externalId, MessageType.MARKET_ORDER.name, matchingResult.cashMovements)

                rabbitSwapQueue.put(MarketOrderWithTrades(marketOrder, matchingResult.marketOrderTrades.toMutableList()))
                trustedLimitOrderReportQueue.put(matchingResult.limitOrdersReport)

                val newOrderBook = OrderBook(marketOrder.assetPairId, !marketOrder.isBuySide(), order.matchedAt!!, genericLimitOrderService.getOrderBook(marketOrder.assetPairId).getCopyOfOrderBook(!marketOrder.isBuySide()))
                orderBookQueue.put(newOrderBook)
                rabbitOrderBookQueue.put(newOrderBook)
                writeResponse(messageWrapper, order, MessageStatus.OK)
            }
            else -> {
            }
        }

        METRICS_LOGGER.log(getMetricLine(order.externalId, order))
        METRICS_LOGGER.log(KeyValue(ME_MARKET_ORDER, (++messagesCount).toString()))

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun parseOld(array: ByteArray): ProtocolMessages.OldMarketOrder {
        return ProtocolMessages.OldMarketOrder.parseFrom(array)
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: MarketOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper.type == MessageType.OLD_MARKET_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()).setRecordId(order.id).build())
        } else if (messageWrapper.type == MessageType.MARKET_ORDER.type) {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason).build())
            }
        } else {
            if (order.price != null) {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setPrice(order.price!!).setStatus(status.type).build())
            } else if (reason == null) {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setStatus(status.type).build())
            } else {
                messageWrapper.writeMarketOrderResponse(ProtocolMessages.MarketOrderResponse.newBuilder().setId(order.externalId).setStatus(status.type).setStatusReason(reason).build())
            }
        }
    }

    fun getMetricLine(uid: String, order: MarketOrder): Line {
        return Line(ME_MARKET_ORDER, arrayOf(
                KeyValue(UID, uid),
                KeyValue(ID, order.id),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, order.clientId),
                KeyValue(ASSET_PAIR, order.assetPairId),
                KeyValue(AMOUNT, order.volume.toString()),
                KeyValue(STRAIGHT, order.straight.toString()),
                KeyValue(STATUS, order.status)
        ))
    }
}