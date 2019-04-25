package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.MarketOrderParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.daos.context.MarketOrderContext
import com.lykke.matching.engine.holders.AssetsHolder
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class MarketOrderContextParser(private val assetsPairsHolder: AssetsPairsHolder,
                               private val assetsHolder: AssetsHolder,
                               private val uuidHolder: UUIDHolder,
                               private val applicationSettingsHolder: ApplicationSettingsHolder) : ContextParser<MarketOrderParsedData> {
    override fun parse(messageWrapper: MessageWrapper): MarketOrderParsedData {
        val protoMessage = parse(messageWrapper.byteArray)

        messageWrapper.messageId = if (protoMessage.hasMessageId()) protoMessage.messageId else protoMessage.uid
        messageWrapper.timestamp = protoMessage.timestamp
        messageWrapper.parsedMessage = protoMessage
        messageWrapper.id = protoMessage.uid
        val processedMessage = ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
        messageWrapper.processedMessage = processedMessage

        messageWrapper.context = getContext(messageWrapper.messageId!!, protoMessage, processedMessage)

        return MarketOrderParsedData(messageWrapper)
    }

    private fun getContext(messageId: String, message: ProtocolMessages.MarketOrder, processedMessage: ProcessedMessage): MarketOrderContext {
        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        val marketOrder = getMarketOrder(message)
        return MarketOrderContext(messageId,
                assetPair,
                getBaseAsset(marketOrder, assetPair)?.let { assetsHolder.getAssetAllowNulls(it) },
                getQuotingAsset(marketOrder, assetPair)?.let { assetsHolder.getAssetAllowNulls(it) },
                getFeeInstruction(message),
                getFeeInstructions(message),
                applicationSettingsHolder.marketOrderPriceDeviationThreshold(message.assetPairId),
                processedMessage,
                marketOrder)
    }

    private fun getMarketOrder(message: ProtocolMessages.MarketOrder): MarketOrder {
        val feeInstruction = getFeeInstruction(message)
        val feeInstructions = getFeeInstructions(message)

        return MarketOrder(uuidHolder.getNextValue(), message.uid, message.assetPairId, message.clientId, BigDecimal.valueOf(message.volume), null,
                OrderStatus.Processing.name, null, Date(message.timestamp), null, null, message.straight, BigDecimal.valueOf(message.reservedLimitVolume),
                feeInstruction, listOfFee(feeInstruction, feeInstructions))
    }

    private fun getFeeInstruction(message: ProtocolMessages.MarketOrder): FeeInstruction? {
        return if (message.hasFee()) FeeInstruction.create(message.fee) else null
    }

    private fun getFeeInstructions(message: ProtocolMessages.MarketOrder): List<NewFeeInstruction> {
        return NewFeeInstruction.create(message.feesList)
    }

    private fun getBaseAsset(order: MarketOrder, assetPair: AssetPair?): String? {
        return if (order.isStraight()) assetPair?.baseAssetId else assetPair?.quotingAssetId
    }

    private fun getQuotingAsset(order: MarketOrder, assetPair: AssetPair?): String? {
        return if (order.isStraight()) assetPair?.quotingAssetId else assetPair?.baseAssetId
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }
}