package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MidPriceHolder
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
                              private val assetsHolder: AssetsHolder) {

    fun create(messageId: String,
               requestId: String,
               messageType: MessageType,
               processedMessage: ProcessedMessage?,
               assetPairsById: Map<String, AssetPair>,
               date: Date,
               logger: Logger,
               assetsById: Map<String, Asset> = getAssetsByIdMap(assetPairsById),
               preProcessorValidationResultsByOrderId: Map<String, OrderValidationResult> = emptyMap(),
               walletOperationsProcessor: WalletOperationsProcessor = balancesHolder.createWalletProcessor(logger),
               orderBooksHolder: CurrentTransactionOrderBooksHolder = genericLimitOrderService.createCurrentTransactionOrderBooksHolder(),
               stopOrderBooksHolder: CurrentTransactionStopOrderBooksHolder = genericStopLimitOrderService.createCurrentTransactionOrderBooksHolder()): ExecutionContext {
        return ExecutionContext(messageId,
                requestId,
                messageType,
                processedMessage,
                assetPairsById,
                assetsById,
                preProcessorValidationResultsByOrderId,
                walletOperationsProcessor,
                orderBooksHolder,
                stopOrderBooksHolder,
                midPriceHolder,
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
