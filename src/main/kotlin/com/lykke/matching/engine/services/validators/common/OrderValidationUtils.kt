package com.lykke.matching.engine.services.validators.common

import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.utils.logging.MetricsLogger

class OrderValidationUtils {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()


        fun isFatalInvalid(validationException: OrderValidationException): Boolean {
            return validationException.orderStatus == OrderStatus.UnknownAsset
        }

        fun validateOrderBookTotalSize(currentOrderBookTotalSize: Int, orderBookMaxTotalSize: Int?) {
            if (orderBookMaxTotalSize != null && currentOrderBookTotalSize >= orderBookMaxTotalSize) {
                val errorMessage = "Order book max total size reached (current: $currentOrderBookTotalSize, max: $orderBookMaxTotalSize)"
                METRICS_LOGGER.logWarning(errorMessage)
                throw OrderValidationException(OrderStatus.OrderBookMaxSizeReached, errorMessage)
            }
        }
    }
}