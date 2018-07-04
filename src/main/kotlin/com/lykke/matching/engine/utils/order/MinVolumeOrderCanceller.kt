package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderOperation
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

@Component
class MinVolumeOrderCanceller @Autowired constructor(private val assetsPairsHolder: AssetsPairsHolder,
                                                     private val genericLimitOrderService: GenericLimitOrderService,
                                                     private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                                                     @Value("#{Config.me.cancelMinVolumeOrders}")
                                                     private val cancelMinVolumeOrders: Boolean): ApplicationRunner {

    companion object {
        private val LOGGER = Logger.getLogger(MinVolumeOrderCanceller::class.java.name)

        private fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    override fun run(args: ApplicationArguments?) {
        if (cancelMinVolumeOrders) {
            cancel()
        }
    }

    fun cancel() {
        val operationId = getOperationId()
        teeLog("Starting order books analyze to cancel min volume orders ($operationId)")

        val operationToOrder = getOperationToOrder()

        teeLog("Starting orders cancellation (orders count: ${operationToOrder.values.size})")
        try {
            val now = Date()
            genericLimitOrdersCancellerFactory.create(LOGGER, now)
                    .preProcessLimitOrders(operationToOrder[OrderOperation.CANCEL] ?: emptyList(),
                            operationToOrder[OrderOperation.REMOVE] ?: emptyList())
                    .applyFull(operationId, operationId, null, MessageType.LIMIT_ORDER, true)
        } catch (e: BalanceException) {
            teeLog("Unable to process wallet operations due to invalid balance: ${e.message}")
            return
        }

        teeLog("Min volume orders cancellation is finished")
    }

    private fun getOperationToOrder(): Map<OrderOperation, List<LimitOrder>> {
        return genericLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap {Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream())}
                .filter{getOrderOperation(it).isPresent}
                .collect(Collectors.groupingBy { getOrderOperation(it).get() })
    }

    private fun getOrderOperation(order: LimitOrder): Optional<OrderOperation> {
        try {
            val assetPair = assetsPairsHolder.getAssetPairAllowNulls(order.assetPairId)

            if (assetPair == null) {
                // assetPair == null means asset pair is not found in dictionary => remove this order (without reserved funds recalculation)
                teeLog("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} is not found")
                return Optional.of(OrderOperation.REMOVE)
            } else if (isOrderVolumeTooSmall(assetPair, order)) {
                teeLog("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} min volume is ${assetPair.minVolume}, remaining volume is ${order.getAbsRemainingVolume()}")
                return Optional.of(OrderOperation.CANCEL)
            }
        } catch (e: Exception) {
            teeLog("Unable to check order (${order.externalId}): ${e.message}. Skipped.")
        }
        return Optional.empty()
    }

    private fun isOrderVolumeTooSmall(assetPair: AssetPair, order: LimitOrder) =
            assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume

    private fun getOperationId() = UUID.randomUUID().toString()
}