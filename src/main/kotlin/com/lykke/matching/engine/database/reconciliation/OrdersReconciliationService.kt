package com.lykke.matching.engine.database.reconciliation

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.common.OrderBookSide
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.database.utils.mapOrdersToOrderBookPersistenceDataList
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(1)
class OrdersReconciliationService(private val config: Config,
                                  private val ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                  private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                  private val persistedOrdersApplicationEventPublisher: SimpleApplicationEventPublisher<OrderBookPersistEvent>,
                                  private val persistedStopApplicationEventPublisher: SimpleApplicationEventPublisher<StopOrderBookPersistEvent>) : ApplicationRunner {
    private companion object {
        val LOGGER = Logger.getLogger(OrdersReconciliationService::class.java.name)
    }

    override fun run(args: ApplicationArguments?) {
        val ordersSecondaryAccessor = ordersDatabaseAccessorsHolder.secondaryAccessor
        if (ordersSecondaryAccessor != null && !config.me.ordersMigration) {
            val currentOrderBookSides = ordersSecondaryAccessor.loadLimitOrders().map { OrderBookSide(it.assetPairId, it.isBuySide()) }.toSet()
            persistedOrdersApplicationEventPublisher.publishEvent(OrderBookPersistEvent(mapOrdersToOrderBookPersistenceDataList(ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders(), currentOrderBookSides, LOGGER)))
        }

        val stopOrdersSecondaryAccessor = stopOrdersDatabaseAccessorsHolder.secondaryAccessor
        if (stopOrdersSecondaryAccessor != null && !config.me.ordersMigration) {
            val currentStopOrderBookSides = stopOrdersSecondaryAccessor.loadStopLimitOrders().map { OrderBookSide(it.assetPairId, it.isBuySide()) }.toSet()
            persistedStopApplicationEventPublisher.publishEvent(StopOrderBookPersistEvent(mapOrdersToOrderBookPersistenceDataList(stopOrdersDatabaseAccessorsHolder.primaryAccessor.loadStopLimitOrders(), currentStopOrderBookSides, LOGGER)))
        }
    }
}