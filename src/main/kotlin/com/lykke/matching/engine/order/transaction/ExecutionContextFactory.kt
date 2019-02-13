package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import org.slf4j.Logger
import org.springframework.stereotype.Component
import java.util.*

@Component
class ExecutionContextFactory(private val walletOperationsProcessorFactory: WalletOperationsProcessorFactory,
                              private val genericLimitOrderService: GenericLimitOrderService,
                              private val midPriceHolder: MidPriceHolder,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val assetsHolder: AssetsHolder,
                              private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) {

    fun create(executionContext: ExecutionContext): ExecutionContext {
        return ExecutionContext(executionContext.messageId,
                executionContext.requestId,
                executionContext.messageType,
                executionContext.processedMessage,
                executionContext.assetPairsById,
                executionContext.assetsById,
                executionContext.preProcessorValidationResultsByOrderId,
                walletOperationsProcessorFactory.create(executionContext.logger, executionContext.walletOperationsProcessor),
                CurrentTransactionOrderBooksHolder(executionContext.orderBooksHolder),
                CurrentTransactionStopOrderBooksHolder(executionContext.stopOrderBooksHolder),
                CurrentTransactionMidPriceHolder(executionContext.currentTransactionMidPriceHolder, priceDeviationThresholdHolder),
                executionContext.date,
                executionContext.logger,
                executionContext.controlsLogger,
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
               controlsLogger: Logger,
               assetsById: Map<String, Asset> = getAssetsByIdMap(assetPairsById),
               preProcessorValidationResultsByOrderId: Map<String, OrderValidationResult> = emptyMap()): ExecutionContext {
        return ExecutionContext(messageId,
                requestId,
                messageType,
                processedMessage,
                assetPairsById,
                assetsById,
                preProcessorValidationResultsByOrderId,
                walletOperationsProcessorFactory.create(logger),
                genericLimitOrderService.createCurrentTransactionOrderBooksHolder(),
                genericStopLimitOrderService.createCurrentTransactionOrderBooksHolder(),
                CurrentTransactionMidPriceHolder(midPriceHolder, priceDeviationThresholdHolder),
                date,
                logger,
                controlsLogger)
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