package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashOperationContextParser(private val assetsHolder: AssetsHolder,
                                 private val applicationSettingsCache: ApplicationSettingsCache) : ContextParser<CashOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): CashOperationParsedData {
        val message = ProtocolMessages.CashOperation.parseFrom(messageWrapper.byteArray)

        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.bussinesId
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid.toString()

        val asset = assetsHolder.getAsset(message.assetId)
        messageWrapper.context = CashOperationContext(message.uid.toString(),
                messageWrapper.messageId!!,
                getWalletOperation(message),
                asset,
                applicationSettingsCache.isAssetDisabled(asset.assetId),
                message.bussinesId,
                message.clientId
        )

        return CashOperationParsedData(messageWrapper)
    }

    private fun getWalletOperation(message: ProtocolMessages.CashOperation): WalletOperation {
        return WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), BigDecimal.valueOf(message.amount), BigDecimal.ZERO)
    }
}