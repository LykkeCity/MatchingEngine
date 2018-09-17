package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.messages.MessageType
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
                                    val applicationSettingsCache: ApplicationSettingsCache,
                                    @Qualifier("singleLimitOrderContextPreprocessorLogger")
                                    val logger: ThrottlingLogger) : ContextParser<SingleLimitOrderParsedData> {
    override fun parse(messageWrapper: MessageWrapper): SingleLimitOrderParsedData {

        val context = parseMessage(messageWrapper)

        messageWrapper.context = context
        messageWrapper.id = context.uid
        messageWrapper.messageId = context.messageId
        messageWrapper.timestamp = context.processedMessage?.timestamp
        messageWrapper.processedMessage = context.processedMessage

        return SingleLimitOrderParsedData(messageWrapper)
    }

    fun getStopOrderContext(messageId: String, order: LimitOrder): SingleLimitOrderContext {
        return getContext(messageId, null, order, false,  null)
    }

    private fun getContext(messageId: String, uid: String?,
                           order: LimitOrder, cancelOrders: Boolean,
                           processedMessage: ProcessedMessage?): SingleLimitOrderContext {
        val builder = SingleLimitOrderContext.Builder()
        val assetPair = getAssetPair(order.assetPairId)

        builder.uid(uid)
                .messageId(messageId)
                .limitOrder(order)
                .assetPair(assetPair)
                .baseAsset(getBaseAsset(assetPair))
                .baseAssetDisabled(applicationSettingsCache.isAssetDisabled(assetPair.baseAssetId))
                .quotingAssetDisabled(applicationSettingsCache.isAssetDisabled(assetPair.quotingAssetId))
                .quotingAsset(getQuotingAsset(assetPair))
                .trustedClient(getTrustedClient(builder.limitOrder.clientId))
                .limitAsset(getLimitAsset(order, assetPair))
                .cancelOrders(cancelOrders)
                .processedMessage(processedMessage)

        return builder.build()
    }

    private fun getLimitAsset(order: LimitOrder, assetPair: AssetPair): Asset {
        return assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
    }

    private fun getTrustedClient(clientId: String): Boolean {
        return applicationSettingsCache.isTrustedClient(clientId)
    }

    fun getAssetPair(assetPairId: String): AssetPair {
        return assetsPairsHolder.getAssetPair(assetPairId)
    }

    private fun getBaseAsset(assetPair: AssetPair): Asset {
        return assetsHolder.getAsset(assetPair.baseAssetId)
    }

    private fun getQuotingAsset(assetPair: AssetPair): Asset {
        return assetsHolder.getAsset(assetPair.quotingAssetId)
    }

    private fun parseMessage(messageWrapper: MessageWrapper): SingleLimitOrderContext {
        return if (messageWrapper.type == MessageType.OLD_LIMIT_ORDER.type) {
            parseOldMessage(messageWrapper)
        } else {
            parseNewMessage(messageWrapper)
        }
    }

    private fun parseOldMessage(messageWrapper: MessageWrapper): SingleLimitOrderContext {
        val oldMessage = parseOldLimitOrder(messageWrapper.byteArray)
        val uid = UUID.randomUUID().toString()
        val messageId = if (oldMessage.hasMessageId()) oldMessage.messageId else oldMessage.uid.toString()

        val limitOrder = LimitOrder(uid, oldMessage.uid.toString(), oldMessage.assetPairId, oldMessage.clientId, BigDecimal.valueOf(oldMessage.volume),
                BigDecimal.valueOf(oldMessage.price), OrderStatus.InOrderBook.name, null, Date(oldMessage.timestamp), null, BigDecimal.valueOf(oldMessage.volume), null,
                type = LimitOrderType.LIMIT, lowerLimitPrice = null, lowerPrice = null, upperLimitPrice = null, upperPrice = null, previousExternalId = null,
                timeInForce = null, expiryTime = null)

        logger.info("Got old limit order messageId: $messageId id: ${oldMessage.uid}, client ${oldMessage.clientId}")

        return getContext(messageId, oldMessage.uid.toString(),
                limitOrder, oldMessage.cancelAllPreviousLimitOrders,
                ProcessedMessage(messageWrapper.type, oldMessage.timestamp, messageId))
    }

    private fun parseNewMessage(messageWrapper: MessageWrapper): SingleLimitOrderContext {
        val message = parseLimitOrder(messageWrapper.byteArray)
        val messageId = if (message.hasMessageId()) message.messageId else message.uid

        val limitOrder = createOrder(message)

        val singleLimitOrderContext = getContext(messageId, message.uid,
                limitOrder, message.cancelAllPreviousLimitOrders,
                ProcessedMessage(messageWrapper.type, message.timestamp, messageId))

        logger.info("Got limit order  messageId: $messageId, id: ${message.uid}, client ${message.clientId}")

        return singleLimitOrderContext
    }

    private fun parseLimitOrder(array: ByteArray): ProtocolMessages.LimitOrder {
        return ProtocolMessages.LimitOrder.parseFrom(array)
    }

    private fun parseOldLimitOrder(array: ByteArray): ProtocolMessages.OldLimitOrder {
        return ProtocolMessages.OldLimitOrder.parseFrom(array)
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
                previousExternalId = null,
                timeInForce = if (message.hasTimeInForce()) OrderTimeInForce.getByExternalId(message.timeInForce) else null,
                expiryTime = if (message.hasExpiryTime()) Date(message.expiryTime) else null)
    }
}