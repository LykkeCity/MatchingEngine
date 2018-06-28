package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCanceller
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.annotation.PostConstruct

@Component
class AllOrdersCanceller @Autowired constructor(private val assetsPairsHolder: AssetsPairsHolder,
                                                private val genericLimitOrderService: GenericLimitOrderService,
                                                private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                                private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                                                @Value("#{Config.me.cancelAllOrders}") private val cancelAllOrders: Boolean){

    private val genericLimitOrdersCanceller: GenericLimitOrdersCanceller

    companion object {
        private val LOGGER = Logger.getLogger(AllOrdersCanceller::class.java.name)

        private fun teeLog(message: String) {
            println(message)
            LOGGER.info(message)
        }
    }

    enum class OrderOperation {
        CANCEL, REMOVE
    }

    init {
        genericLimitOrdersCanceller = genericLimitOrdersCancellerFactory.create(LOGGER, Date())
    }

    @PostConstruct
    fun initialize() {
        if(cancelAllOrders) {
            cancelAllOrders()
        }
    }

    fun cancelAllOrders() {
        val operationId = getOperationId()
        teeLog("Starting cancel all orders in all order books, operation Id: ($operationId)")

        preProcessLimitOrders()
        preProcessStopOrders()

        genericLimitOrdersCanceller.applyFull(operationId, operationId, null, MessageType.LIMIT_ORDER.name, true)
        teeLog("Completed to cancel all orders")
    }

    private fun preProcessLimitOrders() {
        val operationToOrder = getOperationToLimitOrders()

        val ordersToCancel = operationToOrder[OrderOperation.CANCEL] ?: emptyList()
        val ordersToRemove = operationToOrder[OrderOperation.REMOVE] ?: emptyList()

        teeLog("Start cancel of all limit orders orders to cancel count: ${ordersToCancel.size}, " +
                "orders to remove count: ${ordersToRemove.size}")
        genericLimitOrdersCanceller.preProcessLimitOrders(ordersToCancel, ordersToRemove)
    }

    private fun getOperationToLimitOrders(): Map<OrderOperation, List<LimitOrder>> {
        return genericLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap { Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream()) }
                .collect(Collectors.groupingBy(::getOrderOperation))
    }

    private fun preProcessStopOrders() {
        val operationToOrder = getOperationToStopLimitOrders()
        val ordersToCancel = operationToOrder[OrderOperation.CANCEL] ?: emptyList()
        val ordersToRemove = operationToOrder[OrderOperation.REMOVE] ?: emptyList()

        teeLog("Start cancel of all stop orders orders to cancel: ${ordersToCancel.size}, orders to remove count: ${ordersToRemove.size}")
        genericLimitOrdersCanceller.preProcessStopLimitOrders(ordersToCancel,
                ordersToRemove)
    }

    private fun getOperationToStopLimitOrders(): Map<OrderOperation, List<LimitOrder>> {
        return genericStopLimitOrderService.getAllOrderBooks()
                .values
                .stream()
                .map { it.copy() }
                .flatMap { Stream.concat(it.getSellOrderBook().stream(), it.getBuyOrderBook().stream()) }
                .collect(Collectors.groupingBy(::getOrderOperation))
    }

    private fun getOrderOperation(limitOrder: LimitOrder): OrderOperation {
        assetsPairsHolder.getAssetPairAllowNulls(limitOrder.assetPairId) ?: return OrderOperation.REMOVE
        return OrderOperation.CANCEL
    }

    private fun getOperationId() = UUID.randomUUID().toString()
}