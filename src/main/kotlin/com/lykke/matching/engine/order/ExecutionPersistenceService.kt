package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component

@Component
class ExecutionPersistenceService(private val persistenceManager: PersistenceManager) {

    fun persist(messageWrapper: MessageWrapper?,
                executionContext: ExecutionContext,
                sequenceNumber: Long? = null): Boolean {
        if (messageWrapper?.triedToPersist == true) {
            executionContext.error("There already was attempt to persist data")
            return messageWrapper.persisted
        }
        val midPricePersistenceData =  MidPricePersistenceData(executionContext.getMidPrices().mapIndexed{ index, element ->
            MidPrice(element.assetPairId, element.midPrice, element.timestamp + index)}, executionContext.removeAllMidPrices)
        val persisted = persistenceManager.persist(PersistenceData(executionContext.walletOperationsProcessor.persistenceData(),
                executionContext.processedMessage,
                executionContext.orderBooksHolder.getPersistenceData(),
                executionContext.stopOrderBooksHolder.getPersistenceData(),
                sequenceNumber, midPricePersistenceData))
        messageWrapper?.triedToPersist = true
        messageWrapper?.persisted = persisted
        if (persisted) {
            executionContext.apply()
        }
        return persisted
    }
}