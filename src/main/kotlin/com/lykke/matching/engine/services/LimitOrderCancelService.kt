package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.NewLimitOrder
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
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class LimitOrderCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                              private val limitOrderReportQueue: BlockingQueue<JsonSerializable>,
                              private val assetsHolder: AssetsHolder,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val balancesHolder: BalancesHolder,
                              private val orderBookQueue: BlockingQueue<OrderBook>,
                              private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val order: NewLimitOrder?
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrderCancel
            LOGGER.debug("Got old limit order (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

            genericLimitOrderService.cancelLimitOrder(message.limitOrderId.toString(), true)
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setUid(message.uid))
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrderCancel
            LOGGER.debug("Got limit order (id: ${message.limitOrderId}) cancel request id: ${message.uid}")

            order = genericLimitOrderService.cancelLimitOrder(message.limitOrderId, true)

            if (order != null) {
                val now = Date()
                val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = order.reservedLimitVolume ?: if (order.isBuySide()) order.getAbsRemainingVolume() * order.price else order.getAbsRemainingVolume()

                val balance = balancesHolder.getBalance(order.clientId, limitAsset)
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)
                val newReservedBalance = RoundingUtils.parseDouble(reservedBalance - limitVolume, assetsHolder.getAsset(limitAsset).accuracy).toDouble()
                balancesHolder.updateReservedBalance(order.clientId, limitAsset, newReservedBalance)
                balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid,
                        MessageType.LIMIT_ORDER_CANCEL.name,
                        now,
                        listOf(ClientBalanceUpdate(order.clientId, limitAsset, balance, balance, reservedBalance, newReservedBalance)),
                        messageWrapper.messageId!!))
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setId(message.uid)
                        .setStatus(MessageStatus.OK.type))

                val report = LimitOrdersReport(messageWrapper.messageId!!)
                report.orders.add(LimitOrderWithTrades(order))
                limitOrderReportQueue.put(report)

                val rabbitOrderBook = OrderBook(order.assetPairId, order.isBuySide(), now, genericLimitOrderService.getOrderBook(order.assetPairId).copy().getOrderBook(order.isBuySide()))
                orderBookQueue.put(rabbitOrderBook)
                rabbitOrderBookQueue.put(rabbitOrderBook)
            } else {
                LOGGER.info("Unable to find order id: ${message.limitOrderId}")
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setId(message.uid)
                        .setStatus(MessageStatus.LIMIT_ORDER_NOT_FOUND.type))
            }
        }
    }

    private fun getMessageUid(messageWrapper: MessageWrapper): Long? {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrderCancel
            return message.uid
        }

        return null
    }

    private fun parseOldLimitOrderCancel(array: ByteArray): ProtocolMessages.OldLimitOrderCancel {
        return ProtocolMessages.OldLimitOrderCancel.parseFrom(array)
    }

    private fun parseLimitOrderCancel(array: ByteArray): ProtocolMessages.LimitOrderCancel {
        return ProtocolMessages.LimitOrderCancel.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            val message =  parseOldLimitOrderCancel(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseLimitOrderCancel(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setUid(getMessageUid(messageWrapper)!!))
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(messageWrapper.messageId!!)
                    .setStatus(status.type))
        }
    }
}
