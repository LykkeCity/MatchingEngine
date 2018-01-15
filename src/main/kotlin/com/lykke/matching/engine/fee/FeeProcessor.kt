package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.LinkedList
import java.util.UUID

class FeeProcessor(private val balancesHolder: BalancesHolder,
                   private val assetsHolder: AssetsHolder,
                   private val assetsPairsHolder: AssetsPairsHolder,
                   private val genericLimitOrderService: GenericLimitOrderService) {

    companion object {
        private val LOGGER = Logger.getLogger(FeeProcessor::class.java.name)
    }

    fun processMakerFee(feeInstructions: List<FeeInstruction>,
                        receiptOperation: WalletOperation,
                        operations: MutableList<WalletOperation>,
                        invertCoef: Double? = null,
                        balances: MutableMap<String, MutableMap<String, Double>>? = null) =
            processFee(feeInstructions, receiptOperation, operations, invertCoef, true, balances)

    fun processFee(feeInstructions: List<FeeInstruction>?,
                   receiptOperation: WalletOperation,
                   operations: MutableList<WalletOperation>,
                   invertCoef: Double? = null) = processFee(feeInstructions, receiptOperation, operations, invertCoef, false)

    private fun processFee(feeInstructions: List<FeeInstruction>?,
                           receiptOperation: WalletOperation,
                           operations: MutableList<WalletOperation>,
                           invertCoef: Double?,
                           isMakerFee: Boolean,
                           externalBalances: MutableMap<String, MutableMap<String, Double>>? = null): List<FeeTransfer> {
        if (feeInstructions?.isNotEmpty() != true) {
            return listOf()
        }
        val receiptOperationWrapper = ReceiptOperationWrapper(receiptOperation)
        val balances = HashMap<String, MutableMap<String, Double>>() // clientId -> assetId -> balance
        externalBalances?.let {
            balances.putAll(it.mapValues { HashMap<String, Double>(it.value) })
        }
        val newOperations = LinkedList(operations)
        val feeTransfers = feeInstructions.mapNotNull { feeInstruction ->
            if (isMakerFee) {
                when (feeInstruction) {
                    is LimitOrderFeeInstruction -> processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.makerSizeType, feeInstruction.makerSize, balances, invertCoef)
                    is NewLimitOrderFeeInstruction -> processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.makerSizeType, feeInstruction.makerSize, balances, invertCoef)
                    else -> throw FeeException("Fee instruction should be instance of LimitOrderFeeInstruction")
                }
            } else {
                processFee(feeInstruction, receiptOperationWrapper, newOperations, feeInstruction.sizeType, feeInstruction.size, balances, invertCoef)
            }
        }
        val totalFeeAmount = feeTransfers.sumByDouble {
            if (it.asset == receiptOperation.assetId) it.volume else 0.0
        }
        if (receiptOperation.amount >= 0 && totalFeeAmount > 0 && totalFeeAmount >= receiptOperation.amount) {
            throw FeeException("Total fee amount should be less than operation amount (total fee: ${RoundingUtils.roundForPrint(totalFeeAmount)}, operation amount ${RoundingUtils.roundForPrint(receiptOperation.amount)})")
        }
        externalBalances?.putAll(balances)
        operations.clear()
        operations.addAll(newOperations)
        return feeTransfers
    }

    private fun processFee(feeInstruction: FeeInstruction,
                           receiptOperationWrapper: ReceiptOperationWrapper,
                           operations: MutableList<WalletOperation>,
                           feeSizeType: FeeSizeType?,
                           feeSize: Double?,
                           balances: MutableMap<String, MutableMap<String, Double>>,
                           invertCoef: Double?): FeeTransfer? {
        if (feeInstruction.type == FeeType.NO_FEE) {
            return null
        }
        if (feeSizeType == null || feeSize == null || feeInstruction.targetClientId == null) {
            throw FeeException("Invalid fee instruction (size type: $feeSizeType, size: $feeSize, targetClientId: ${feeInstruction.targetClientId }")
        }
        val receiptOperation = receiptOperationWrapper.baseReceiptOperation
        val operationAsset = assetsHolder.getAsset(receiptOperation.assetId)

        val baseAssetFeeAmount = RoundingUtils.round(when (feeSizeType) {
            FeeSizeType.PERCENTAGE -> receiptOperation.amount * feeSize
            FeeSizeType.ABSOLUTE -> feeSize
        }, operationAsset.accuracy, true)

        if (receiptOperation.amount >= 0) {
            if (baseAssetFeeAmount <= 0.0 || baseAssetFeeAmount >= receiptOperation.amount) {
                throw FeeException("Base asset fee amount ($baseAssetFeeAmount) should be in (0, ${receiptOperation.amount})")
            }
        }
        // in case of cash out receipt operation has a negative amount, but fee amount should be positive
        val absBaseAssetFeeAmount = Math.abs(baseAssetFeeAmount)

        return when (feeInstruction.type) {
            FeeType.CLIENT_FEE -> processClientFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, operationAsset, invertCoef, balances)
            FeeType.EXTERNAL_FEE -> processExternalFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, operationAsset, invertCoef, balances)
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
                                   operationAsset: Asset,
                                   invertCoef: Double?,
                                   balances: MutableMap<String, MutableMap<String, Double>>): FeeTransfer? {
        if (feeInstruction.sourceClientId == null) {
            throw FeeException("Source client is null for external fee")
        }
        val receiptOperation = receiptOperationWrapper.baseReceiptOperation
        val feeAsset = getFeeAsset(feeInstruction, operationAsset)
        val anotherAsset = operationAsset.assetId != feeAsset.assetId
        val feeAmount = RoundingUtils.round(if (anotherAsset) absBaseAssetFeeAmount * (invertCoef ?: computeInvertCoef(operationAsset.assetId, feeAsset.assetId)) else absBaseAssetFeeAmount, feeAsset.accuracy, true)

        val clientBalances = balances.getOrPut(feeInstruction.sourceClientId) { HashMap() }
        val balance = clientBalances.getOrPut(feeAsset.assetId) { balancesHolder.getAvailableBalance(feeInstruction.sourceClientId, feeAsset.assetId) }
        return if (balance < feeAmount) {
            processClientFee(feeInstruction, receiptOperationWrapper, operations, absBaseAssetFeeAmount, operationAsset, invertCoef, balances)
        } else {
            clientBalances[feeAsset.assetId] = RoundingUtils.parseDouble(balance - feeAmount, feeAsset.accuracy).toDouble()
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.sourceClientId, feeAsset.assetId, receiptOperation.dateTime, -feeAmount, isFee = true))
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, feeAsset.assetId, receiptOperation.dateTime, feeAmount, isFee = true))
            FeeTransfer(receiptOperation.externalId, feeInstruction.sourceClientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, feeAsset.assetId)
        }
    }

    private fun processClientFee(feeInstruction: FeeInstruction,
                                 receiptOperationWrapper: ReceiptOperationWrapper,
                                 operations: MutableList<WalletOperation>,
                                 absBaseAssetFeeAmount: Double,
                                 operationAsset: Asset,
                                 invertCoef: Double?,
                                 balances: MutableMap<String, MutableMap<String, Double>>): FeeTransfer? {
        val receiptOperation = receiptOperationWrapper.currentReceiptOperation
        val feeAsset = getFeeAsset(feeInstruction, operationAsset)
        val anotherAsset = operationAsset.assetId != feeAsset.assetId

        val feeAmount = RoundingUtils.round(if (anotherAsset) absBaseAssetFeeAmount * (invertCoef ?: computeInvertCoef(operationAsset.assetId, feeAsset.assetId)) else absBaseAssetFeeAmount, feeAsset.accuracy, true)
        val clientBalances = balances.getOrPut(receiptOperation.clientId) { HashMap() }
        val balance = clientBalances.getOrPut(feeAsset.assetId) { balancesHolder.getAvailableBalance(receiptOperation.clientId, feeAsset.assetId) }

        if (anotherAsset) {
            if (balance < feeAmount) {
                throw FeeException("Not enough funds for fee (balance: $balance, feeAmount: $feeAmount)")
            }
            clientBalances[feeAsset.assetId] = RoundingUtils.parseDouble(balance - feeAmount, feeAsset.accuracy).toDouble()
            operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, receiptOperation.clientId, feeAsset.assetId, receiptOperation.dateTime, -feeAmount, isFee = true))
        } else {
            val baseReceiptOperationAmount = receiptOperationWrapper.baseReceiptOperation.amount
            if (baseReceiptOperationAmount < 0) {
                val newBalance = RoundingUtils.parseDouble(balance - absBaseAssetFeeAmount, feeAsset.accuracy).toDouble()
                if (newBalance < Math.abs(baseReceiptOperationAmount)) {
                    throw FeeException("Not enough funds for fee (balance: $balance, withdrawalAmount: ${baseReceiptOperationAmount}, feeAmount: $absBaseAssetFeeAmount)")
                } else {
                    clientBalances[feeAsset.assetId] = newBalance
                }
            }
            operations.remove(receiptOperation)
            val newReceiptOperation = WalletOperation(receiptOperation.id, receiptOperation.externalId, receiptOperation.clientId, receiptOperation.assetId, receiptOperation.dateTime, RoundingUtils.parseDouble(receiptOperation.amount - absBaseAssetFeeAmount, operationAsset.accuracy).toDouble())
            operations.add(newReceiptOperation)
            receiptOperationWrapper.currentReceiptOperation = newReceiptOperation
        }

        operations.add(WalletOperation(UUID.randomUUID().toString(), receiptOperation.externalId, feeInstruction.targetClientId!!, feeAsset.assetId, receiptOperation.dateTime, feeAmount, isFee = true))

        return FeeTransfer(receiptOperation.externalId, receiptOperation.clientId, feeInstruction.targetClientId, receiptOperation.dateTime, feeAmount, feeAsset.assetId)
    }

    private fun getFeeAsset(feeInstruction: FeeInstruction, operationAsset: Asset): Asset {
        val assetIds = when (feeInstruction) {
            is NewFeeInstruction -> feeInstruction.assetIds
            else -> listOf()
        }
        return if (assetIds.isNotEmpty()) assetsHolder.getAsset(assetIds.first()) else operationAsset
    }

    private fun computeInvertCoef(operationAssetId: String, feeAssetId: String): Double {
        val assetPair = try {
            assetsPairsHolder.getAssetPair(operationAssetId, feeAssetId)
        } catch (e: Exception) {
            throw FeeException(e.message ?: "Unable to get asset pair for ($operationAssetId, $feeAssetId})")
        }
        val orderBook = genericLimitOrderService.getOrderBook(assetPair.assetPairId)
        val price = if (assetPair.baseAssetId == feeAssetId) orderBook.getAskPrice() else orderBook.getBidPrice()
        if (price <= 0.0) {
            throw FeeException("Unable to get a price to convert to fee asset (price is 0 or order book is empty)")
        }
        return if (assetPair.baseAssetId == feeAssetId) price else 1 / price
    }

}
