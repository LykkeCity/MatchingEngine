package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class StopLimitOrderProcessor(private val limitOrderService: GenericLimitOrderService,
                              private val stopLimitOrderService: GenericStopLimitOrderService,
                              private val genericLimitOrderProcessor: GenericLimitOrderProcessor,
                              private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                              private val assetsHolder: AssetsHolder,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val balancesHolder: BalancesHolder,
                              applicationSettingsCache: ApplicationSettingsCache,
                              private val LOGGER: Logger) {

    private val validator = LimitOrderValidator(assetsPairsHolder, assetsHolder, applicationSettingsCache)

    fun processStopOrder(messageWrapper: MessageWrapper, order: NewLimitOrder, isCancelOrders: Boolean, now: Date) {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAsset = assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitVolume = BigDecimal.valueOf(if (order.isBuySide())
            NumberUtils.round(order.volume * (order.upperPrice ?: order.lowerPrice)!!, limitAsset.accuracy, true)
        else
            order.getAbsVolume())

        val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
        var cancelVolume = 0.0
        val ordersToCancel = mutableListOf<NewLimitOrder>()
        val newStopOrderBook = stopLimitOrderService.getOrderBook(order.assetPairId).getOrderBook(order.isBuySide()).toMutableList()
        if (isCancelOrders) {
            stopLimitOrderService.searchOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                ordersToCancel.add(orderToCancel)
                newStopOrderBook.remove(orderToCancel)
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(orderToCancel))
                cancelVolume += orderToCancel.reservedLimitVolume!!
            }
        }

        val availableBalance = NumberUtils.parseDouble(balancesHolder.getAvailableBalance(order.clientId, limitAsset.assetId, cancelVolume), limitAsset.accuracy)
        try {
            validateOrder(order, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info("${orderInfo(order)} ${e.message}")
            order.status = e.orderStatus.name
            val messageStatus = MessageStatusUtils.toMessageStatus(e.orderStatus)
            val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
            if (cancelVolume > 0) {
                walletOperationsProcessor.preProcess(listOf(WalletOperation(UUID.randomUUID().toString(),
                        order.externalId,
                        order.clientId,
                        limitAsset.assetId, now, 0.0, -cancelVolume)))
            }
            val orderBooksPersistenceData = if (ordersToCancel.isNotEmpty())
                OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId, order.isBuySide(), newStopOrderBook)),
                        emptyList(),
                        ordersToCancel) else null
            messageWrapper.processedMessagePersisted = true
            val updated = walletOperationsProcessor.persistBalances(ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!),
                    null,
                    orderBooksPersistenceData)
            if (updated) {
                walletOperationsProcessor.apply().sendNotification(order.externalId, MessageType.LIMIT_ORDER.name, messageWrapper.messageId!!)
                stopLimitOrderService.cancelStopLimitOrders(order.assetPairId, ordersToCancel)
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setId(order.externalId)
                        .setMatchingEngineId(order.id)
                        .setMessageId(messageWrapper.messageId)
                        .setStatus(messageStatus.type))

                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                clientLimitOrderReportQueue.put(clientLimitOrdersReport)
            } else {
                writePersistenceErrorResponse(messageWrapper, order)
            }
            return
        }

        val orderBook = limitOrderService.getOrderBook(order.assetPairId)
        val bestBidPrice = orderBook.getBidPrice()
        val bestAskPrice = orderBook.getAskPrice()

        var price: Double? = null
        if (order.lowerLimitPrice != null && (order.isBuySide() && bestAskPrice > 0 && bestAskPrice <= order.lowerLimitPrice ||
                !order.isBuySide() && bestBidPrice > 0 && bestBidPrice <= order.lowerLimitPrice)) {
            price = order.lowerPrice
        } else if(order.upperLimitPrice != null && (order.isBuySide() && bestAskPrice >= order.upperLimitPrice ||
                !order.isBuySide() && bestBidPrice >= order.upperLimitPrice)) {
            price = order.upperPrice
        }

        if (price != null) {
            LOGGER.info("Process stop order ${order.externalId}, client ${order.clientId} immediately (bestBidPrice=$bestBidPrice, bestAskPrice=$bestAskPrice)")
            order.status = OrderStatus.InOrderBook.name
            order.price = price

            genericLimitOrderProcessor.processLimitOrder(messageWrapper.messageId!!,
                    ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!),
                    order,
                    now,
                    0.0)
            return
        }

        val walletOperations = mutableListOf<WalletOperation>()
        walletOperations.add(WalletOperation(UUID.randomUUID().toString(),
                order.externalId,
                order.clientId,
                limitAsset.assetId, now, 0.0, -cancelVolume))
        walletOperations.add(WalletOperation(UUID.randomUUID().toString(),
                order.externalId,
                order.clientId,
                limitAsset.assetId, now, 0.0, limitVolume.toDouble()))
        val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
        walletOperationsProcessor.preProcess(walletOperations)

        order.reservedLimitVolume = limitVolume.toDouble()
        newStopOrderBook.add(order)
        messageWrapper.processedMessagePersisted = true
        val updated = walletOperationsProcessor.persistBalances(ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!),
                null,
                OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId, order.isBuySide(), newStopOrderBook)),
                        listOf(order),
                        ordersToCancel))

        if (!updated) {
            writePersistenceErrorResponse(messageWrapper, order)
            return
        }

        walletOperationsProcessor.apply().sendNotification(order.externalId, MessageType.LIMIT_ORDER.name, messageWrapper.messageId!!)
        stopLimitOrderService.cancelStopLimitOrders(order.assetPairId, ordersToCancel)
        stopLimitOrderService.addStopOrder(order)

        clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))

        writeResponse(messageWrapper, order, MessageStatus.OK)
        LOGGER.info("${orderInfo(order)} added to stop order book")

        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrderReportQueue.put(clientLimitOrdersReport)
        }
    }

    private fun validateOrder(order: NewLimitOrder, assetPair: AssetPair, availableBalance: BigDecimal, limitVolume: BigDecimal) {
        validator.validateFee(order)
        validator.validateAssets(assetPair)
        validator.validateLimitPrices(order)
        validator.validateVolume(order)
        validator.checkBalance(availableBalance, limitVolume)
        validator.validateVolumeAccuracy(order)
        validator.validatePriceAccuracy(order)
    }

    private fun orderInfo(order: NewLimitOrder): String {
        return "Stop limit order (id: ${order.externalId})"
    }

    private fun writeResponse(messageWrapper: MessageWrapper, order: NewLimitOrder, status: MessageStatus, reason: String? = null) {
        val builder = ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(order.id)
                .setStatus(status.type)
        if (reason != null) {
            builder.statusReason = reason
        }
        messageWrapper.writeNewResponse(builder)
    }

    private fun writePersistenceErrorResponse(messageWrapper: MessageWrapper, order: NewLimitOrder) {
        val message = "Unable to save result data"
        LOGGER.error("$message (stop limit order id ${order.externalId})")
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(order.id)
                .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                .setStatusReason(message))
    }
}