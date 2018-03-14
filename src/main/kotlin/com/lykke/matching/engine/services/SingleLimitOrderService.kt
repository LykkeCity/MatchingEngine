package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.LimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class SingleLimitOrderService(limitOrderProcessorFactory: LimitOrderProcessorFactory): AbstractService {

    @Deprecated("Use primary constructor")
    constructor(limitOrderService: GenericLimitOrderService,
                trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                orderBookQueue: BlockingQueue<OrderBook>,
                rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                assetsHolder: AssetsHolder,
                assetsPairsHolder: AssetsPairsHolder,
                balancesHolder: BalancesHolder,
                applicationSettingsCache: ApplicationSettingsCache,
                lkkTradesQueue: BlockingQueue<List<LkkTrade>>) :
            this(LimitOrderProcessorFactory(limitOrderService,
                    trustedClientLimitOrderReportQueue,
                    clientLimitOrderReportQueue,
                    orderBookQueue,
                    rabbitOrderBookQueue,
                    assetsHolder,
                    assetsPairsHolder,
                    balancesHolder,
                    applicationSettingsCache,
                    lkkTradesQueue))

    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0

    private val limitOrderProcessor = limitOrderProcessorFactory.create(LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }

        val order: NewLimitOrder
        val now = Date()
        val isCancelOrders: Boolean

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val oldMessage = messageWrapper.parsedMessage!! as ProtocolMessages.OldLimitOrder
            val uid = UUID.randomUUID().toString()
            order = NewLimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, oldMessage.volume,
                    oldMessage.price, OrderStatus.InOrderBook.name, Date(oldMessage.timestamp), now, oldMessage.volume, null,
                    type = LimitOrderType.LIMIT, lowerLimitPrice = null, lowerPrice = null, upperLimitPrice = null, upperPrice = null)

            LOGGER.info("Got old limit order id: ${oldMessage.uid}, client ${oldMessage.clientId}, assetPair: ${oldMessage.assetPairId}, volume: ${RoundingUtils.roundForPrint(oldMessage.volume)}, price: ${RoundingUtils.roundForPrint(oldMessage.price)}, cancel: ${oldMessage.cancelAllPreviousLimitOrders}")

            isCancelOrders = oldMessage.cancelAllPreviousLimitOrders
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.LimitOrder
            order = createOrder(message, now)
            LOGGER.info("Got limit order ${incomingMessageInfo(message, order)}")
            isCancelOrders = message.cancelAllPreviousLimitOrders
        }

        when(order.type) {
            LimitOrderType.LIMIT -> if (!limitOrderProcessor.processLimitOrder(messageWrapper, order, isCancelOrders, now)) return
            LimitOrderType.STOP_LIMIT -> if (!limitOrderProcessor.processStopOrder(messageWrapper, order, isCancelOrders, now)) return
        }

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    private fun incomingMessageInfo(message: ProtocolMessages.LimitOrder, order: NewLimitOrder): String {
        return "id: ${message.uid}" +
                ", type: ${order.type}" +
                ", client: ${message.clientId}" +
                ", assetPair: ${message.assetPairId}" +
                ", volume: ${RoundingUtils.roundForPrint(message.volume)}" +
                ", price: ${RoundingUtils.roundForPrint(order.price)}" +
                (if (order.lowerLimitPrice != null) ", lowerLimitPrice: ${RoundingUtils.roundForPrint(order.lowerLimitPrice)}" else "") +
                (if (order.lowerPrice != null) ", lowerPrice: ${RoundingUtils.roundForPrint(order.lowerPrice)}" else "") +
                (if (order.upperLimitPrice != null) ", upperLimitPrice: ${RoundingUtils.roundForPrint(order.upperLimitPrice)}" else "") +
                (if (order.upperPrice != null) ", upperPrice: ${RoundingUtils.roundForPrint(order.upperPrice)}" else "") +
                ", cancel: ${message.cancelAllPreviousLimitOrders}" +
                ", fee: ${order.fee}" +
                ", fees: ${order.fees}"
    }

    private fun createOrder(message: ProtocolMessages.LimitOrder, now: Date): NewLimitOrder {
        val type = if (message.hasType()) LimitOrderType.getByExternalId(message.type) else LimitOrderType.LIMIT
        val status = when(type) {
            LimitOrderType.LIMIT -> OrderStatus.InOrderBook
            LimitOrderType.STOP_LIMIT -> OrderStatus.Pending
        }
        return NewLimitOrder(UUID.randomUUID().toString(),
                message.uid,
                message.assetPairId,
                message.clientId,
                message.volume,
                if (message.hasPrice()) message.price else 0.0,
                status.name,
                Date(message.timestamp),
                now,
                message.volume,
                null,
                fee = if (message.hasFee()) LimitOrderFeeInstruction.create(message.fee) else null,
                fees = NewLimitOrderFeeInstruction.create(message.feesList),
                type = type,
                lowerLimitPrice = if (message.hasLowerLimitPrice()) message.lowerLimitPrice else null,
                lowerPrice = if (message.hasLowerPrice()) message.lowerPrice else null,
                upperLimitPrice = if (message.hasUpperLimitPrice()) message.upperLimitPrice else null,
                upperPrice = if (message.hasUpperPrice()) message.upperPrice else null)
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            val message =  parseOldLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageWrapper.messageId!!.toLong()).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        }
    }
}
