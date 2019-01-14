package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.util.Date

@Component
class ExecutionContextFactory(private val balancesHolder: BalancesHolder,
                              private val genericLimitOrderService: GenericLimitOrderService,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val assetsHolder: AssetsHolder) {

    fun create(executionContext: ExecutionContext): ExecutionContext {
        return ExecutionContext(executionContext.messageId,
                executionContext.requestId,
                executionContext.messageType,
                executionContext.processedMessage,
                executionContext.assetPairsById,
                executionContext.assetsById,
                executionContext.preProcessorValidationResultsByOrderId,
                balancesHolder.createWalletProcessor(executionContext.walletOperationsProcessor.currentTransactionBalancesHolder.createCurrenTransactionBalancesHolder(),
                        executionContext.logger),
                CurrentTransactionOrderBooksHolder(executionContext.orderBooksHolder),
                CurrentTransactionStopOrderBooksHolder(executionContext.stopOrderBooksHolder),
                executionContext.date,
                executionContext.logger,
                executionContext.tradeIndex,
                executionContext)
    }

    fun create(messageId: String,
               requestId: String,
               messageType: MessageType,
               processedMessage: ProcessedMessage?,
               assetPairsById: Map<String, AssetPair>,
               date: Date,
               logger: Logger,
               assetsById: Map<String, Asset> = getAssetsByIdMap(assetPairsById),
               preProcessorValidationResultsByOrderId: Map<String, OrderValidationResult> = emptyMap()): ExecutionContext {
        return ExecutionContext(messageId,
                requestId,
                messageType,
                processedMessage,
                assetPairsById,
                assetsById,
                preProcessorValidationResultsByOrderId,
                balancesHolder.createWalletProcessor(logger),
                genericLimitOrderService.createCurrentTransactionOrderBooksHolder(),
                genericStopLimitOrderService.createCurrentTransactionOrderBooksHolder(),
                date,
                logger)
    }

    private fun getAssetsByIdMap(assetPairsById: Map<String, AssetPair>): Map<String, Asset> {
        return assetPairsById.values
                .flatMapTo(mutableSetOf()) {
                    listOf(it.baseAssetId, it.quotingAssetId)
                }
                .asSequence()
                .map { assetsHolder.getAsset(it) }
                .groupBy { it.assetId }
                .mapValues { it.value.single() }
    }
}
