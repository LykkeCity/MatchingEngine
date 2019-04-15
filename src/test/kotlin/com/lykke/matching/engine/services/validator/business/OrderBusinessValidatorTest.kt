package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.business.OrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderBusinessValidatorTest {

    @Autowired
    private lateinit var orderBusinessValidator: OrderBusinessValidator

    @Test(expected = OrderValidationException::class)
    fun testInvalidBalance() {
        try {
            //when
            orderBusinessValidator.validateBalance(BigDecimal.valueOf(10.0), BigDecimal.valueOf(11.0))
        } catch (e: OrderValidationException) {
            //then
            Assert.assertEquals(OrderStatus.NotEnoughFunds, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidBalance() {
        //when
        orderBusinessValidator.validateBalance(BigDecimal.valueOf(10.0), BigDecimal.valueOf(9.0))
    }
}