package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessageUtils
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashInOutContextParser(private val assetsHolder: AssetsHolder) : ContextParser {
    override fun parse(messageWrapper: MessageWrapper): MessageWrapper {
        val operationId = UUID.randomUUID().toString()

        val message = ProtocolMessages.CashInOutOperation.parseFrom(messageWrapper.byteArray)

        messageWrapper.id = message.id
        messageWrapper.messageId = message.messageId

        messageWrapper.context = CashInOutContext(message.id,
                if (message.hasMessageId()) message.messageId else message.id,
                operationId,
                message.clientId,
                if (ProcessedMessageUtils.isDeduplicationNotNeeded(MessageType.CASH_IN_OUT_OPERATION.type)) null
                else ProcessedMessage(MessageType.CASH_IN_OUT_OPERATION.type, message.timestamp, message.messageId),
                NewFeeInstruction.create(message.feesList),
                WalletOperation(operationId, message.id, message.clientId, message.assetId,
                        Date(message.timestamp), BigDecimal.valueOf(message.volume), BigDecimal.ZERO),
                assetsHolder.getAsset(message.assetId),
                BigDecimal.valueOf(message.volume), Date())

        return messageWrapper
    }
}