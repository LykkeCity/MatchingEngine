package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.util.*

@Component
class ExecutionContextFactory(private val balancesHolder: BalancesHolder,
                              private val genericLimitOrderService: GenericLimitOrderService,
                              private val genericStopLimitOrderService: GenericStopLimitOrderService,
                              private val midPriceHolder: MidPriceHolder,
                              private val assetsHolder: AssetsHolder,
                              private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) {

    fun create(messageId: String,
               requestId: String,
               messageType: MessageType,
               processedMessage: ProcessedMessage?,
               assetPairsById: Map<String, AssetPair>,
               date: Date,
               logger: Logger,
               controlsLogger: Logger,
               assetsById: Map<String, Asset> = getAssetsByIdMap(assetPairsById),
               currentTransactionMidPriceHolder: CurrentTransactionMidPriceHolder = CurrentTransactionMidPriceHolder(midPriceHolder, priceDeviationThresholdHolder),
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
                currentTransactionMidPriceHolder,
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