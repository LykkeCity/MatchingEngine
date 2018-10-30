package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.CashInOutOperation
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashInOutContextParser(private val assetsHolder: AssetsHolder) : ContextParser<CashInOutParsedData> {
    override fun parse(messageWrapper: MessageWrapper): CashInOutParsedData {
        val operationId = UUID.randomUUID().toString()

        val message = ProtocolMessages.CashInOutOperation.parseFrom(messageWrapper.byteArray)

        messageWrapper.id = message.id
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.processedMessage = ProcessedMessage(MessageType.CASH_IN_OUT_OPERATION.type, message.timestamp, messageWrapper.messageId!!)

        messageWrapper.context = CashInOutContext(
                if (message.hasMessageId()) message.messageId else message.id,
                messageWrapper.processedMessage!!,
                CashInOutOperation(operationId, message.id, message.clientId, assetsHolder.getAssetAllowNulls(message.assetId),
                        Date(message.timestamp), BigDecimal.valueOf(message.volume),
                        feeInstructions = NewFeeInstruction.create(message.feesList)))

        return CashInOutParsedData(messageWrapper, message.assetId)
    }
}