package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.junit.Test
import kotlin.test.assertTrue

class OrderValidationUtilsTest {
    @Test
    fun isFatalInvalid() {
        assertTrue(OrderValidationUtils.isFatalInvalid(OrderValidationException(OrderStatus.UnknownAsset)))
    }
}