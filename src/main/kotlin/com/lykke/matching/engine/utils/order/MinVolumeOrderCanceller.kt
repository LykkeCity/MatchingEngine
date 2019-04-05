package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

@Component
@Order(5)
class MinVolumeOrderCanceller @Autowired constructor(private val assetsPairsHolder: AssetsPairsHolder,
                                                     private val genericLimitOrderService: GenericLimitOrderService,
                                                     private val limitOrdersCancelExecutor: LimitOrdersCancelExecutor,
                                                     @Value("#{Config.me.cancelMinVolumeOrders}")
                                                     private val cancelMinVolumeOrders: Boolean) : ApplicationRunner {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(MinVolumeOrderCanceller::class.java.name)
    }

    override fun run(args: ApplicationArguments?) {
        if (cancelMinVolumeOrders) {
            cancel()
        }
    }

    fun cancel() {
        val operationId = generateOperationId()
        LOGGER.info("Starting order books analyze to cancel min volume orders ($operationId)")
        try {

            val ordersToCancel = getOrdersToCancel()
            LOGGER.info("Starting orders cancellation (orders count: ${ordersToCancel.size})")

            limitOrdersCancelExecutor.cancelOrdersAndApply(CancelRequest(ordersToCancel,
                    emptyList(),
                    operationId,
                    operationId,
                    MessageType.LIMIT_ORDER,
                    Date(),
                    null,
                    null,
                    LOGGER))
        } catch (e: BalanceException) {
            LOGGER.error("Unable to process wallet operations due to invalid balance: ${e.message}", e)
            return
        }

        LOGGER.info("Min volume orders cancellation is finished")
    }

    private fun getOrdersToCancel(): List<LimitOrder> {
        return genericLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap { Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream()) }
                .filter { isOrderToCancel(it) }
                .collect(Collectors.toList())
    }

    private fun isOrderToCancel(order: LimitOrder): Boolean {
        return try {
            val assetPair = assetsPairsHolder.getAssetPairAllowNulls(order.assetPairId)
            when {
                assetPair == null -> {
                    // assetPair == null means asset pair is not found in dictionary => remove this order (without reserved funds recalculation)
                    LOGGER.info("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} is not found")
                    true
                }
                isOrderVolumeTooSmall(assetPair, order) -> {
                    LOGGER.info("Order (id: ${order.externalId}, clientId: ${order.clientId}) is added to cancel: asset pair ${order.assetPairId} min volume is ${assetPair.minVolume}, remaining volume is ${order.getAbsRemainingVolume()}")
                    true
                }
                else -> false
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to check order (${order.externalId}): ${e.message}. Skipped.", e)
            false
        }
    }

    private fun isOrderVolumeTooSmall(assetPair: AssetPair, order: LimitOrder) =
            assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume

    private fun generateOperationId() = UUID.randomUUID().toString()
}