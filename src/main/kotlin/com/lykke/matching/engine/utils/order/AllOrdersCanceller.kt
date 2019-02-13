package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
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
@Order(4)
class AllOrdersCanceller @Autowired constructor(private val genericLimitOrderService: GenericLimitOrderService,
                                                private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                private val limitOrdersCancelExecutor: LimitOrdersCancelExecutor,
                                                @Value("#{Config.me.cancelAllOrders}") private val cancelAllOrders: Boolean) : ApplicationRunner {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AllOrdersCanceller::class.java.name)
    }

    override fun run(args: ApplicationArguments?) {
        if (cancelAllOrders) {
            cancelAllOrders()
        }
    }

    fun cancelAllOrders() {
        val operationId = generateOperationId()
        LOGGER.info("Starting cancel all orders in all order books, operation Id: ($operationId)")

        val limitOrdersToCancel = getLimitOrders()
        val stopLimitOrdersToCancel = getStopLimitOrders()

        LOGGER.info("Limit orders count: ${limitOrdersToCancel.size}, " +
                "stop limit orders count: ${stopLimitOrdersToCancel.size}")

        limitOrdersCancelExecutor.cancelOrdersAndApply(CancelRequest(limitOrdersToCancel,
                stopLimitOrdersToCancel,
                operationId,
                operationId,
                MessageType.LIMIT_ORDER,
                Date(),
                null,
                null,
                LOGGER,
                LOGGER,
                true))

        LOGGER.info("Completed to cancel all orders")
    }

    private fun getLimitOrders(): List<LimitOrder> {
        return genericLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap { Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream()) }
                .collect(Collectors.toList())
    }

    private fun getStopLimitOrders(): List<LimitOrder> {
        return genericStopLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap { Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream()) }
                .collect(Collectors.toList())
    }

    private fun generateOperationId() = UUID.randomUUID().toString()
}