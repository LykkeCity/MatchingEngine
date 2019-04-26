package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class OrderBookTotalSizeInitialChecker(private val genericLimitOrderService: GenericLimitOrderService,
                                       private val genericStopLimitOrderService: GenericStopLimitOrderService,
                                       private val orderBookMaxTotalSizeHolder: OrderBookMaxTotalSizeHolder) {

    @PostConstruct
    fun check() {
        val orderBookMaxTotalSize = orderBookMaxTotalSizeHolder.get() ?: return
        val orderBookTotalSize = genericLimitOrderService.getTotalSize() + genericStopLimitOrderService.getTotalSize()
        if (orderBookMaxTotalSize < orderBookTotalSize) {
            throw IllegalStateException("Current order book total size ($orderBookTotalSize) is greater than cunfigured maximum size ($orderBookMaxTotalSize)")
        }
    }
}