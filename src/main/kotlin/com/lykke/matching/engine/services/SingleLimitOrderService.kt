package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.ME_LIMIT_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class SingleLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                              private val limitOrderReportQueue: BlockingQueue<JsonSerializable>,
                              private val orderBookQueue: BlockingQueue<OrderBook>,
                              private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                              private val assetsHolder: AssetsHolder,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val negativeSpreadAssets: Set<String>,
                              private val balancesHolder: BalancesHolder,
                              private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor): AbstractService<ProtocolMessages.OldLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val order: LimitOrder

        val now = Date()
        val isCancelOrders: Boolean
        val limitOrdersReport = LimitOrdersReport()
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage = parseOldLimitOrder(messageWrapper.byteArray)
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, oldMessage.volume,
                    oldMessage.price, OrderStatus.InOrderBook.name, Date(oldMessage.timestamp), now, oldMessage.volume, null)

            LOGGER.info("Got old limit order id: ${oldMessage.uid}, client ${oldMessage.clientId}, assetPair: ${oldMessage.assetPairId}, volume: ${RoundingUtils.roundForPrint(oldMessage.volume)}, price: ${RoundingUtils.roundForPrint(oldMessage.price)}")

            isCancelOrders = oldMessage.cancelAllPreviousLimitOrders
        } else {
            val message = parseLimitOrder(messageWrapper.byteArray)
            val uid = UUID.randomUUID().toString()
            order = LimitOrder(uid, message.uid, message.assetPairId, message.clientId, message.volume,
                    message.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, message.volume, null)

            LOGGER.info("Got limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, price: ${RoundingUtils.roundForPrint(message.price)}")

            isCancelOrders = message.cancelAllPreviousLimitOrders
        }

        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val limitVolume = if (order.isBuySide()) RoundingUtils.round(order.getAbsVolume() * order.price, assetsHolder.getAsset(limitAsset).accuracy, true) else order.getAbsVolume()

        val balance = balancesHolder.getBalance(order.clientId, limitAsset)
        val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

        val orderBook = limitOrderService.getOrderBook(order.assetPairId).copy()

        var cancelVolume = 0.0
        if (isCancelOrders) {
            limitOrderService.getAllPreviousOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                limitOrderService.cancelLimitOrder(orderToCancel.externalId)
                orderBook.removeOrder(orderToCancel)
                limitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += if (orderToCancel.isBuySide()) orderToCancel.remainingVolume * orderToCancel.price else orderToCancel.remainingVolume
            }
        }

        if (balance - (reservedBalance - cancelVolume) < limitVolume) {
            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} not enough funds to reserve")
            order.status = OrderStatus.NotEnoughFunds.name
            rejectOrder(reservedBalance, cancelVolume, limitAsset, order, balance, limitOrdersReport, orderBook, messageWrapper, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, now)

            if (isCancelOrders) {
                val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
                orderBookQueue.put(rabbitOrderBook)
                rabbitOrderBookQueue.put(rabbitOrderBook)
            }
            return
        } else if (orderBook.leadToNegativeSpreadForClient(order)) {
            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} lead to negative spread")
            order.status = OrderStatus.LeadToNegativeSpread.name
            rejectOrder(reservedBalance, cancelVolume, limitAsset, order, balance, limitOrdersReport, orderBook, messageWrapper, MessageStatus.LEAD_TO_NEGATIVE_SPREAD, now)

            if (isCancelOrders) {
                val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
                orderBookQueue.put(rabbitOrderBook)
                rabbitOrderBookQueue.put(rabbitOrderBook)
            }
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()))
            val limitOrder = matchingResult.order as LimitOrder
            when (OrderStatus.valueOf(matchingResult.order.status)) {
                OrderStatus.NoLiquidity -> {
                    limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.NO_LIQUIDITY)
                }
                OrderStatus.ReservedVolumeGreaterThanBalance -> {
                    limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE, "Reserved volume is higher than available balance")
                }
                OrderStatus.NotEnoughFunds -> {
                    limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    writeResponse(messageWrapper, limitOrder, MessageStatus.NOT_ENOUGH_FUNDS)
                }
                OrderStatus.Matched,
                OrderStatus.Processing-> {
                    limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)
                    matchingResult.cancelledLimitOrders.forEach { it ->
                        it.status = OrderStatus.NotEnoughFunds.name
                    }
                    limitOrderService.moveOrdersToDone(ArrayList<LimitOrder>(matchingResult.cancelledLimitOrders))

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    if (matchingResult.uncompletedLimitOrder != null) {
                        matchingResult.orderBook.put(matchingResult.uncompletedLimitOrder)
                    }

                    limitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)
                    limitOrderService.updateOrderBook(order.assetPairId, !order.isBuySide())

                    marketOrderDatabaseAccessor.addLkkTrades(matchingResult.lkkTrades)

                    limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder, matchingResult.marketOrderTrades.map { it ->
                      LimitTradeInfo(it.marketClientId, it.marketAsset, it.marketVolume, it.price, now, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume)
                    }.toMutableList()))

                    if (matchingResult.limitOrdersReport != null) {
                        limitOrdersReport.orders.addAll(matchingResult.limitOrdersReport.orders)
                    }

                    val walletOperations = matchingResult.cashMovements.toMutableList()

                    if (order.status == OrderStatus.Processing.name) {
                        orderBook.addOrder(order)
                        limitOrderService.addOrder(order)
                        limitOrderService.setOrderBook(order.assetPairId, order.isBuySide(), orderBook.getOrderBook(order.isBuySide()))
                        limitOrderService.updateOrderBook(order.assetPairId, order.isBuySide())

                        order.reservedLimitVolume = if (order.isBuySide()) RoundingUtils.round(order.getAbsRemainingVolume() * order.price , assetsHolder.getAsset(limitAsset).accuracy, true) else order.getAbsRemainingVolume()
                        val newReservedBalance =  RoundingUtils.parseDouble(reservedBalance - cancelVolume + order.reservedLimitVolume!!, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
                        walletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, limitAsset, now, 0.0, newReservedBalance))
                        limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), if (order.isBuySide()) orderBook.getBidPrice() else orderBook.getAskPrice(), now))
                    }

                    balancesHolder.processWalletOperations(order.externalId, MessageType.LIMIT_ORDER.name, walletOperations)

                    val newOrderBook = OrderBook(limitOrder.assetPairId, limitOrder.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(limitOrder.assetPairId).getCopyOfOrderBook(limitOrder.isBuySide()))
                    orderBookQueue.put(newOrderBook)
                    rabbitOrderBookQueue.put(newOrderBook)
                    val oppositeOrderBook = OrderBook(limitOrder.assetPairId, !limitOrder.isBuySide(), order.lastMatchTime!!, limitOrderService.getOrderBook(limitOrder.assetPairId).getCopyOfOrderBook(!limitOrder.isBuySide()))
                    orderBookQueue.put(oppositeOrderBook)
                    rabbitOrderBookQueue.put(oppositeOrderBook)
                    writeResponse(messageWrapper, order, MessageStatus.OK)
                }
                else -> {
                }
            }
            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} matched")
        } else {
            order.reservedLimitVolume = limitVolume

            orderBook.addOrder(order)
            limitOrderService.addOrder(order)
            limitOrderService.setOrderBook(order.assetPairId, order.isBuySide(), orderBook.getOrderBook(order.isBuySide()))
            limitOrderService.updateOrderBook(order.assetPairId, order.isBuySide())

            limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), if (order.isBuySide()) orderBook.getBidPrice() else orderBook.getAskPrice(), now))
            limitOrdersReport.orders.add(LimitOrderWithTrades(order))

            val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - cancelVolume + limitVolume, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
            balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance, reservedBalance, newReservedBalance))))

            val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, limitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
            orderBookQueue.put(rabbitOrderBook)
            rabbitOrderBookQueue.put(rabbitOrderBook)
            LOGGER.info("Limit order id: ${order.externalId}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} added to order book")
        }

        limitOrderService.setOrderBook(order.assetPairId, orderBook)

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(now.time).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(MessageStatus.OK.type).build())
        }

        if (limitOrdersReport.orders.isNotEmpty()) {
            limitOrderReportQueue.put(limitOrdersReport)
        }

        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER, (++messagesCount).toString()))
    }

    private fun rejectOrder(reservedBalance: Double, cancelVolume: Double, limitAsset: String, order: LimitOrder, balance: Double, limitOrdersReport: LimitOrdersReport, orderBook: AssetOrderBook, messageWrapper: MessageWrapper, status: MessageStatus, now: Date) {
        val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - cancelVolume, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
        balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
        balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance, reservedBalance, newReservedBalance))))

        limitOrdersReport.orders.add(LimitOrderWithTrades(order))
        limitOrderService.setOrderBook(order.assetPairId, orderBook)

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(now.time).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
        }

        if (limitOrdersReport.orders.isNotEmpty()) {
            limitOrderReportQueue.put(limitOrdersReport)
        }

        METRICS_LOGGER.log(KeyValue(ME_LIMIT_ORDER, (++messagesCount).toString()))
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: LimitOrder, status: MessageStatus, reason: String? = null) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(order.externalId.toLong()).build())
        } else {
            if (reason == null) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).build())
            } else {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(order.externalId).setMatchingEngineId(order.id).setStatus(status.type).setStatusReason(reason).build())
            }
        }
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }
}
