
package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
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

        val balance = balancesHolder.getBalance(order.clientId, limitAsset.assetId)
        val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset.assetId)
        val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
        var cancelVolume = 0.0
        val ordersToCancel = mutableListOf<NewLimitOrder>()
        if (isCancelOrders) {
            stopLimitOrderService.searchOrders(order.clientId, order.assetPairId, order.isBuySide()).forEach { orderToCancel ->
                ordersToCancel.add(orderToCancel)
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
            var updated = true
            if (cancelVolume > 0) {
                val newReservedBalance = NumberUtils.parseDouble(reservedBalance - cancelVolume, limitAsset.accuracy).toDouble()
                updated = balancesHolder.updateReservedBalance(order.clientId, limitAsset.assetId, newReservedBalance)
                if (updated) {
                    balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId, MessageType.LIMIT_ORDER.name, Date(), listOf(ClientBalanceUpdate(order.clientId, limitAsset.assetId, balance, balance, reservedBalance, newReservedBalance)), messageWrapper.messageId!!))
                }
            }

            if (updated) {
                stopLimitOrderService.cancelStopLimitOrders(order.assetPairId, order.isBuySide(), ordersToCancel)
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

            genericLimitOrderProcessor.processLimitOrder(messageWrapper.messageId!!, order, now, 0.0)
            return
        }

        val newReservedBalance = NumberUtils.parseDouble(reservedBalance - cancelVolume + limitVolume.toDouble(), limitAsset.accuracy).toDouble()
        val updated = balancesHolder.updateReservedBalance(order.clientId, limitAsset.assetId, newReservedBalance)
        if (!updated) {
            writePersistenceErrorResponse(messageWrapper, order)
            return
        }

        balancesHolder.sendBalanceUpdate(BalanceUpdate(order.externalId,
                MessageType.LIMIT_ORDER.name,
                now,
                listOf(ClientBalanceUpdate(order.clientId,
                        limitAsset.assetId,
                        balance,
                        balance,
                        reservedBalance,
                        newReservedBalance)),
                messageWrapper.messageId!!))
        stopLimitOrderService.cancelStopLimitOrders(order.assetPairId, order.isBuySide(), ordersToCancel)

        order.reservedLimitVolume = limitVolume.toDouble()
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