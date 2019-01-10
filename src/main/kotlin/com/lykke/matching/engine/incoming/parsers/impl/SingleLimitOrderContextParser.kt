package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class SingleLimitOrderContextParser(val assetsPairsHolder: AssetsPairsHolder,
                                    val assetsHolder: AssetsHolder,
                                    val applicationSettingsHolder: ApplicationSettingsHolder,
                                    @Qualifier("singleLimitOrderPreProcessingLogger")
                                    val logger: ThrottlingLogger) : ContextParser<SingleLimitOrderParsedData> {

    override fun parse(messageWrapper: MessageWrapper): SingleLimitOrderParsedData {

        val context = parseMessage(messageWrapper)

        messageWrapper.context = context
        messageWrapper.id = context.limitOrder.externalId
        messageWrapper.messageId = context.messageId
        messageWrapper.timestamp = context.processedMessage?.timestamp
        messageWrapper.processedMessage = context.processedMessage

        return SingleLimitOrderParsedData(messageWrapper, context.limitOrder.assetPairId)
    }

    private fun getContext(messageId: String,
                           order: LimitOrder, cancelOrders: Boolean,
                           processedMessage: ProcessedMessage?): SingleLimitOrderContext {
        val builder = SingleLimitOrderContext.Builder()
        val assetPair = getAssetPair(order.assetPairId)

        builder.messageId(messageId)
                .limitOrder(order)
                .assetPair(assetPair)
                .baseAsset(assetPair?.let { getBaseAsset(it) })
                .quotingAsset(assetPair?.let { getQuotingAsset(it) })
                .trustedClient(getTrustedClient(builder.limitOrder.clientId))
                .limitAsset(assetPair?.let { getLimitAsset(order, assetPair) })
                .cancelOrders(cancelOrders)
                .processedMessage(processedMessage)

        return builder.build()
    }

    private fun getLimitAsset(order: LimitOrder, assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
    }

    private fun getTrustedClient(clientId: String): Boolean {
        return applicationSettingsHolder.isTrustedClient(clientId)
    }

    fun getAssetPair(assetPairId: String): AssetPair? {
        return assetsPairsHolder.getAssetPairAllowNulls(assetPairId)
    }

    private fun getBaseAsset(assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(assetPair.baseAssetId)
    }

    private fun getQuotingAsset(assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(assetPair.quotingAssetId)
    }

    private fun parseMessage(messageWrapper: MessageWrapper): SingleLimitOrderContext {
        val message = parseLimitOrder(messageWrapper.byteArray)
        val messageId = if (message.hasMessageId()) message.messageId else message.uid

        val limitOrder = createOrder(message)

        val singleLimitOrderContext = getContext(messageId, limitOrder, message.cancelAllPreviousLimitOrders,
                ProcessedMessage(messageWrapper.type, message.timestamp, messageId))

        logger.info("Got limit order  messageId: $messageId, id: ${message.uid}, client ${message.clientId}")

        return singleLimitOrderContext
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun createOrder(message: ProtocolMessages.LimitOrder): LimitOrder {
        val type = if (message.hasType()) LimitOrderType.getByExternalId(message.type) else LimitOrderType.LIMIT
        val status = when (type) {
            LimitOrderType.LIMIT -> OrderStatus.InOrderBook
            LimitOrderType.STOP_LIMIT -> OrderStatus.Pending
        }
        val feeInstruction = if (message.hasFee()) LimitOrderFeeInstruction.create(message.fee) else null
        val feeInstructions = NewLimitOrderFeeInstruction.create(message.feesList)
        return LimitOrder(UUID.randomUUID().toString(),
                message.uid,
                message.assetPairId,
                message.clientId,
                BigDecimal.valueOf(message.volume),
                if (message.hasPrice()) BigDecimal.valueOf(message.price) else BigDecimal.ZERO,
                status.name,
                null,
                Date(message.timestamp),
                null,
                BigDecimal.valueOf(message.volume),
                null,
                fee = feeInstruction,
                fees = listOfLimitOrderFee(feeInstruction, feeInstructions),
                type = type,
                lowerLimitPrice = if (message.hasLowerLimitPrice()) BigDecimal.valueOf(message.lowerLimitPrice) else null,
                lowerPrice = if (message.hasLowerPrice()) BigDecimal.valueOf(message.lowerPrice) else null,
                upperLimitPrice = if (message.hasUpperLimitPrice()) BigDecimal.valueOf(message.upperLimitPrice) else null,
                upperPrice = if (message.hasUpperPrice()) BigDecimal.valueOf(message.upperPrice) else null,
                previousExternalId = null)
    }
}