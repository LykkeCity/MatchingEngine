package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.Fee
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.HashMap
import java.util.LinkedList
import java.util.UUID

class FeeProcessor(private val balancesHolder: BalancesHolder,
                   private val assetsHolder: AssetsHolder,
                   private val assetsPairsHolder: AssetsPairsHolder,
                   private val genericLimitOrderService: GenericLimitOrderService) {

    companion object {
        private val LOGGER = Logger.getLogger(FeeProcessor::class.java.name)
        private const val FEE_COEF_ACCURACY = 12
    }

    fun processMakerFee(feeInstructions: List<FeeInstruction>,
                        receiptOperation: WalletOperation,
                        operations: MutableList<WalletOperation>,
                        relativeSpread: Double? = null,
                        convertPrices: Map<String, Double> = emptyMap(),
                        balances: MutableMap<String, MutableMap<String, Double>>? = null) =
            processFees(feeInstructions,
                    receiptOperation,
                    operations,
                    MakerFeeCoefCalculator(relativeSpread),
                    convertPrices,
                    true,
                    balances)

    fun processFee(feeInstructions: List<FeeInstruction>?,
                   receiptOperation: WalletOperation,
                   operations: MutableList<WalletOperation>,
                   convertPrices: Map<String, Double> = emptyMap(),
                   balances: MutableMap<String, MutableMap<String, Double>>? = null) =
            processFees(feeInstructions,
                    receiptOperation,
                    operations,
                    DefaultFeeCoefCalculator(),
                    convertPrices,
                    false,
                    balances)

    private fun processFees(feeInstructions: List<FeeInstruction>?,
                           receiptOperation: WalletOperation,
                           operations: MutableList<WalletOperation>,
                           feeCoefCalculator: FeeCoefCalculator,
                           convertPrices: Map<String, Double>,
                           isMakerFee: Boolean,
                           externalBalances: MutableMap<String, MutableMap<String, Double>>? = null): List<Fee> {
        if (feeInstructions?.isNotEmpty() != true) {
            return listOf()
        }
        val receiptOperationWrapper = ReceiptOperationWrapper(receiptOperation)
        val balances = HashMap<String, MutableMap<String, Double>>() // clientId -> assetId -> balance
        externalBalances?.let {
            balances.putAll(it.mapValues { HashMap<String, Double>(it.value) })
        }
        val newOperations = LinkedList(operations)
        if (isMakerFee && feeCoefCalculator !is MakerFeeCoefCalculator) {
            throw FeeException("Fee coef calculator should be instance of MakerFeeCoefCalculator")
        }
        val fees = feeInstructions.map { feeInstruction ->
            val feeTransfer = if (isMakerFee) {
                feeCoefCalculator as MakerFeeCoefCalculator
                when (feeInstruction) {
                    is LimitOrderFeeInstruction -> {
                        feeCoefCalculator.feeModificator = null
                        processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.makerSizeType, feeInstruction.makerSize, feeCoefCalculator.calculate(), balances, convertPrices)
                    }
                    is NewLimitOrderFeeInstruction -> {
                        feeCoefCalculator.feeModificator = feeInstruction.makerFeeModificator
                        processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.makerSizeType, feeInstruction.makerSize, feeCoefCalculator.calculate(), balances, convertPrices)
                    }
                    else -> throw FeeException("Fee instruction should be instance of LimitOrderFeeInstruction")
                }
            } else {
                processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.sizeType, feeInstruction.size, feeCoefCalculator.calculate(), balances, convertPrices)
            }
            Fee(feeInstruction, feeTransfer)
        }
        val totalFeeAmount = fees.sumByDouble { fee ->
            if (fee.transfer != null && fee.transfer.asset == receiptOperation.assetId) fee.transfer.volume else 0.0
        }
        if (totalFeeAmount > 0 && totalFeeAmount > Math.abs(receiptOperation.amount)) {
            throw FeeException("Total fee amount should be not more than ${if (receiptOperation.amount < 0) "abs " else ""}operation amount (total fee: ${RoundingUtils.roundForPrint(totalFeeAmount)}, operation amount ${RoundingUtils.roundForPrint(receiptOperation.amount)})")
        }
        externalBalances?.putAll(balances)
        operations.clear()
        operations.addAll(newOperations)
        return fees
    }

    private fun processFee(feeInstruction: FeeInstruction,
                           receiptOperationWrapper: ReceiptOperationWrapper,
                           operations: MutableList<WalletOperation>,
                           feeSizeType: FeeSizeType?,
                           feeSize: Double?,
                           feeCoef: Double?,
                           balances: MutableMap<String, MutableMap<String, Double>>,
                           convertPrices: Map<String, Double>): FeeTransfer? {
        if (feeInstruction.type == FeeType.NO_FEE) {
            return null
        }
        if (feeSizeType == null || feeSize == null || feeSize < 0.0 || feeInstruction.targetClientId == null) {
            throw FeeException("Invalid fee instruction (size type: $feeSizeType, size: $feeSize, targetClientId: ${feeInstruction.targetClientId })")
        }
        val receiptOperation = receiptOperationWrapper.baseReceiptOperation
        val operationAsset = assetsHolder.getAsset(receiptOperation.assetId)

        val absBaseAssetFeeAmount = RoundingUtils.round(when (feeSizeType) {
        // In case of cash out receipt operation has a negative amount, but fee amount should be positive
            FeeSizeType.PERCENTAGE -> Math.abs(receiptOperation.amount) * feeSize * (feeCoef ?: 1.0)
            FeeSizeType.ABSOLUTE -> feeSize * (feeCoef ?: 1.0)
        }, operationAsset.accuracy, true)

        if (absBaseAssetFeeAmount > Math.abs(receiptOperation.amount)) {
            throw FeeException("Base asset fee amount ($absBaseAssetFeeAmount) should be in [0, ${Math.abs(receiptOperation.amount)}]")
        }

        return when (feeInstruction.type) {
            FeeType.CLIENT_FEE -> processClientFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, feeCoef, operationAsset, convertPrices, balances)
            FeeType.EXTERNAL_FEE -> processExternalFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, feeCoef, operationAsset, convertPrices, balances)
            else -> {
                LOGGER.error("Unknown fee type: ${feeInstruction.type}")
                null
            }
        }
    }

    private fun processExternalFee(feeInstruction: FeeInstruction,
                                   receiptOperationWrapper: ReceiptOperationWrapper,
                                   operations: MutableList<WalletOperation>,
                                   absBaseAssetFeeAmount: Double,
                                   feeCoef: Double?,
                                   operationAsset: Asset,
                                   convertPrices: Map<String, Double>,
                                   balances: MutableMap<String, MutableMap<String, Double>>): FeeTransfer? {
        if (feeInstruction.sourceClientId == null) {
            throw FeeException("Source client is null for external fee")
        }
        val receiptOperation = receiptOperationWrapper.baseReceiptOperation
        val feeAsset = getFeeAsset(feeInstruction, operationAsset)
        val anotherAsset = operationAsset.assetId != feeAsset.assetId
        val feeAmount = RoundingUtils.round(if (anotherAsset) absBaseAssetFeeAmount * computeInvertCoef(operationAsset.assetId, feeAsset.assetId, convertPrices) else absBaseAssetFeeAmount, feeAsset.accuracy, true)

        val clientBalances = balances.getOrPut(feeInstruction.sourceClientId) { HashMap() }
        val balance = clientBalances.getOrPut(feeAsset.assetId) { balancesHolder.getAvailableBalance(feeInstruction.sourceClientId, feeAsset.assetId) }
        return if (balance < feeAmount) {
            processClientFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, feeCoef, operationAsset, convertPrices, balances)
        } else {
            clientBalances[feeAsset.assetId] = RoundingUtils.parseDouble(balance - feeAmount, feeAsset.accuracy).toDouble()
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.sourceClientId, feeAsset.assetId, receiptOperation.dateTime, -feeAmount, isFee = true))
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, feeAsset.assetId, receiptOperation.dateTime, feeAmount, isFee = true))
            FeeTransfer(receiptOperation.externalId, feeInstruction.sourceClientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, feeAsset.assetId, if (feeCoef != null) RoundingUtils.parseDouble(feeCoef, FEE_COEF_ACCURACY).toDouble() else null)
        }
    }

    private fun processClientFee(feeInstruction: FeeInstruction,
                                 receiptOperationWrapper: ReceiptOperationWrapper,
                                 operations: MutableList<WalletOperation>,
                                 absBaseAssetFeeAmount: Double,
                                 feeCoef: Double?,
                                 operationAsset: Asset,
                                 convertPrices: Map<String, Double>,
                                 balances: MutableMap<String, MutableMap<String, Double>>): FeeTransfer? {
        val receiptOperation = receiptOperationWrapper.currentReceiptOperation
        val feeAsset = getFeeAsset(feeInstruction, operationAsset)
        val anotherAsset = operationAsset.assetId != feeAsset.assetId

        val feeAmount = RoundingUtils.round(if (anotherAsset) absBaseAssetFeeAmount * computeInvertCoef(operationAsset.assetId, feeAsset.assetId, convertPrices) else absBaseAssetFeeAmount, feeAsset.accuracy, true)
        val clientBalances = balances.getOrPut(receiptOperation.clientId) { HashMap() }
        val balance = clientBalances.getOrPut(feeAsset.assetId) { balancesHolder.getAvailableBalance(receiptOperation.clientId, feeAsset.assetId) }

        if (anotherAsset) {
            if (balance < feeAmount) {
                throw NotEnoughFundsFeeException("Not enough funds for fee (asset: ${feeAsset.assetId}, available balance: $balance, feeAmount: $feeAmount)")
            }
            clientBalances[feeAsset.assetId] = RoundingUtils.parseDouble(balance - feeAmount, feeAsset.accuracy).toDouble()
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, receiptOperation.clientId, feeAsset.assetId, receiptOperation.dateTime, -feeAmount, isFee = true))
        } else {
            val baseReceiptOperationAmount = receiptOperationWrapper.baseReceiptOperation.amount
            val newReceiptAmount = if (baseReceiptOperationAmount > 0) receiptOperation.amount - absBaseAssetFeeAmount else receiptOperation.amount
            operations.remove(receiptOperation)
            val newReceiptOperation = WalletOperation(receiptOperation.id, receiptOperation.externalId, receiptOperation.clientId, receiptOperation.assetId, receiptOperation.dateTime, RoundingUtils.parseDouble(newReceiptAmount, operationAsset.accuracy).toDouble())
            operations.add(newReceiptOperation)
            receiptOperationWrapper.currentReceiptOperation = newReceiptOperation
        }

        operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, feeAsset.assetId, receiptOperation.dateTime, feeAmount, isFee = true))

        return FeeTransfer(receiptOperation.externalId, receiptOperation.clientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, feeAsset.assetId, if (feeCoef != null) RoundingUtils.parseDouble(feeCoef, FEE_COEF_ACCURACY).toDouble() else null)
    }

    private fun getFeeAsset(feeInstruction: FeeInstruction, operationAsset: Asset): Asset {
        val assetIds = when (feeInstruction) {
            is NewFeeInstruction -> feeInstruction.assetIds
            else -> listOf()
        }
        return if (assetIds.isNotEmpty()) assetsHolder.getAsset(assetIds.first()) else operationAsset
    }

    private fun computeInvertCoef(operationAssetId: String, feeAssetId: String, convertPrices: Map<String, Double>): Double {
        val assetPair = try {
            assetsPairsHolder.getAssetPair(operationAssetId, feeAssetId)
        } catch (e: Exception) {
            throw FeeException(e.message ?: "Unable to get asset pair for ($operationAssetId, $feeAssetId})")
        }
        val price = if (convertPrices.containsKey(assetPair.assetPairId)) {
            convertPrices[assetPair.assetPairId]!!
        } else {
            val orderBook = genericLimitOrderService.getOrderBook(assetPair.assetPairId)
            val askPrice = orderBook.getAskPrice()
            val bidPrice = orderBook.getBidPrice()
            if (askPrice > 0.0 && bidPrice > 0.0) (askPrice + bidPrice) / 2 else 0.0
        }
        if (price <= 0.0) {
            throw FeeException("Unable to get a price to convert to fee asset (price is not positive or order book is empty)")
        }
        return if (assetPair.baseAssetId == feeAssetId) 1 / price else price
    }

}
