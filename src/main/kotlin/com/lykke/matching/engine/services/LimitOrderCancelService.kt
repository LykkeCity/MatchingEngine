package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.business.LimitOrderCancelOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*
import java.util.stream.Collectors

@Service
class LimitOrderCancelService(private val genericLimitOrderService: GenericLimitOrderService,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val validator: LimitOrderCancelOperationBusinessValidator,
                              private val limitOrdersCancelHelper: LimitOrdersCancelHelper,
                              private val midPriceHolder: MidPriceHolder,
                              private val assetsPairsHolder: AssetsPairsHolder,
                              private val persistenceManager: PersistenceManager) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val now = Date()
        val context = messageWrapper.context as LimitOrderCancelOperationContext

        if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER_CANCEL.type) {
            processOldLimitOrderCancelMessage(messageWrapper, context, now)
            return
        }

        LOGGER.debug("Got limit order cancel request (id: ${context.uid}, orders: ${context.limitOrderIds})")
        val typeToOrder = getLimitOrderTypeToLimitOrders(context.limitOrderIds)

        try {
            validator.performValidation(typeToOrder, context)
        } catch (e: ValidationException) {
            LOGGER.info("Business validation failed: ${context.messageId}, details: ${e.message}")
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        val updateSuccessful = limitOrdersCancelHelper.cancelOrders(LimitOrdersCancelHelper.CancelRequest(context.uid,
                context.messageId,
                context.messageType,
                typeToOrder[LimitOrderType.LIMIT],
                typeToOrder[LimitOrderType.STOP_LIMIT], now, context.processedMessage, false))


        limitOrdersCancelHelper.processPersistResults(updateSuccessful, messageWrapper, context.messageId)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(status.type))
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //nothing to do
    }

    private fun getLimitOrderTypeToLimitOrders(orderIds: Set<String>): Map<LimitOrderType, List<LimitOrder>> {
        return orderIds.stream()
                .map(::getOrder)
                .filter { limitOrder -> limitOrder != null }
                .map { t -> t!! }
                .collect(Collectors.groupingBy { limitOrder: LimitOrder -> limitOrder.type!! })
    }

    private fun getOrder(orderId: String): LimitOrder? {
        return genericLimitOrderService.getOrder(orderId) ?: genericStopLimitOrderService.getOrder(orderId)
    }

    private fun processOldLimitOrderCancelMessage(messageWrapper: MessageWrapper, context: LimitOrderCancelOperationContext, now: Date) {
        LOGGER.debug("Got old limit  order messageId: ${context.messageId}  (id: ${context.limitOrderIds}) cancel request id: ${context.uid}")

        val limitOrderId = context.limitOrderIds.first().toString()
        val order = genericLimitOrderService.getOrder(limitOrderId)
        if (order != null) {
            val newOrderBook = genericLimitOrderService.getOrderBook(order.assetPairId).copy()
            newOrderBook.removeOrder(order)

            val midPricePersistenceData = newOrderBook.getMidPrice()?.let {
                MidPricePersistenceData(MidPrice(order.assetPairId, it, now.time))
            }

            val updated = persistenceManager.persist(PersistenceData(null,
                    messageWrapper.processedMessage,
                    OrderBooksPersistenceData(listOf(OrderBookPersistenceData(order.assetPairId,
                            order.isBuySide(),
                            newOrderBook.getCopyOfOrderBook(order.isBuySide()))),
                            emptyList(),
                            listOf(order)),
                    null,
                    null,
                    midPricePersistenceData))
            if (updated) {
                updateMidPriceInCache(order.assetPairId, newOrderBook.getMidPrice(), now)
                genericLimitOrderService.cancelLimitOrder(Date(), limitOrderId, true)
            }
        }
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
    }

    private fun updateMidPriceInCache(assetPairId: String, midPrice: BigDecimal?, operationTime: Date) {
        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(assetPairId)
        if (assetPair != null && midPrice != null) {
            midPriceHolder.addMidPrice(assetPair, midPrice, operationTime)
        }
    }
}
