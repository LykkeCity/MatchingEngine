package com.lykke.matching.engine.services.validator.settings

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.validator.BalanceUpdateValidatorTest
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.settings.impl.DisabledFunctionalitySettingValidator
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (BalanceUpdateValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DisabledFunctionalitySettingValidatorTest {

    @Autowired
    private lateinit var disabledFunctionalitySettingValidator: DisabledFunctionalitySettingValidator

    @Test(expected = ValidationException::class)
    fun testEmptyRuleIsInvalid() {
        disabledFunctionalitySettingValidator.validate(DisabledFunctionalityRuleDto(null, null, "", null, true, "test", "test"))
    }

    @Test(expected = ValidationException::class)
    fun testMessageTypeDoesNotExist() {
        disabledFunctionalitySettingValidator.validate(DisabledFunctionalityRuleDto(null, null, "", 77, true, "test", "test"))
    }

    @Test(expected = ValidationException::class)
    fun testAssetDoesNotExist() {
        disabledFunctionalitySettingValidator.validate(DisabledFunctionalityRuleDto(null, "TEST", "", 77, true, "test", "test"))
    }

    @Test(expected = ValidationException::class)
    fun testAssetPairDoesNotExist() {
        disabledFunctionalitySettingValidator.validate(DisabledFunctionalityRuleDto(null, "TEST", "", 77, true, "test", "test"))
    }

    @Test
    fun validRule() {
        disabledFunctionalitySettingValidator.validate(DisabledFunctionalityRuleDto(null, "BTC", "BTCUSD", MessageType.MARKET_ORDER.type.toInt(), true, "test", "test"))
    }
}