package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.listOfFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashTransferContextParser(private val assetsHolder: AssetsHolder) : ContextParser {
    override fun parse(messageWrapper: MessageWrapper): MessageWrapper {
        val message = ProtocolMessages.CashTransferOperation.parseFrom(messageWrapper.byteArray)

        val feeInstruction = if (message.hasFee()) FeeInstruction.create(message.fee) else null
        val feeInstructions = NewFeeInstruction.create(message.feesList)

        val transferOperation = TransferOperation(UUID.randomUUID().toString(), message.id,
                message.fromClientId, message.toClientId,
                message.assetId, Date(message.timestamp),
                BigDecimal.valueOf(message.volume), BigDecimal.valueOf(message.overdraftLimit),
                listOfFee(feeInstruction, feeInstructions))

        messageWrapper.id = message.id
        messageWrapper.messageId = message.messageId

        messageWrapper.context =
                CashTransferContext(
                        if (message.hasMessageId()) message.messageId else message.id,
                        feeInstruction,
                        feeInstructions,
                        transferOperation,
                        assetsHolder.getAsset(transferOperation.asset),
                        transferOperation.asset,
                        ProcessedMessage(MessageType.CASH_TRANSFER_OPERATION.type, message.timestamp, message.messageId),
                        Date())

        return messageWrapper
    }
}