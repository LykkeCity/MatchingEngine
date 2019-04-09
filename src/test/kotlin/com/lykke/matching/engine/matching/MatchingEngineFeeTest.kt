package com.lykke.matching.engine.matching

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstruction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MatchingEngineTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MatchingEngineFeeTest : MatchingEngineTest() {

    @Test
    fun testSellLimitOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 121.12)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.21111, volume = 100.0,
                fee = buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.0211111,
                        targetClientId = "Client4"
                ),
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.0211111,
                        targetClientId = "Client4"
                )
        ))

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -200.0,
                fee = buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSize = 0.01,
                        targetClientId = "Client3"
                ),
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        takerSize = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = match(limitOrder, getOrderBook("EURUSD", true))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("Client2", "EUR", BigDecimal.valueOf(-100.0), BigDecimal.ZERO),
                        WalletOperation("Client2", "USD", BigDecimal.valueOf(119.89), BigDecimal.ZERO),
                        WalletOperation("Client3", "USD", BigDecimal.valueOf(1.22), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client1", "EUR", BigDecimal.valueOf(97.8888), BigDecimal.ZERO),
                        WalletOperation( "Client1", "USD", BigDecimal.valueOf(-121.11), BigDecimal.valueOf(-121.11)),
                        WalletOperation( "Client4", "EUR", BigDecimal.valueOf(2.1112), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testBuyLimitOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        val limitOrder = buildLimitOrder(clientId = "Client1", price = 1.2, volume = 200.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        takerSize = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = match(limitOrder, getOrderBook("EURUSD", false))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client1", "EUR", BigDecimal.valueOf(99.0), BigDecimal.ZERO),
                        WalletOperation( "Client1", "USD", BigDecimal.valueOf(-120.0), BigDecimal.ZERO),
                        WalletOperation( "Client3", "EUR", BigDecimal.valueOf(1.0), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client2", "EUR", BigDecimal.valueOf(-100.0), BigDecimal.valueOf(-100.0)),
                        WalletOperation( "Client2", "USD", BigDecimal.valueOf(117.6), BigDecimal.ZERO),
                        WalletOperation( "Client4", "USD", BigDecimal.valueOf(2.4), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testSellMarketOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 120.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = 100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        val limitOrder = buildMarketOrder(clientId = "Client2", volume = -100.0,
                fees = buildFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        size = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = match(limitOrder, getOrderBook("EURUSD", true))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client2", "EUR", BigDecimal.valueOf(-100.0), BigDecimal.ZERO),
                        WalletOperation( "Client2", "USD", BigDecimal.valueOf(118.8), BigDecimal.ZERO),
                        WalletOperation( "Client3", "USD", BigDecimal.valueOf(1.2), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client1", "EUR", BigDecimal.valueOf(98.0), BigDecimal.ZERO),
                        WalletOperation( "Client1", "USD", BigDecimal.valueOf(-120.0), BigDecimal.valueOf(-120.0)),
                        WalletOperation( "Client4", "EUR", BigDecimal.valueOf(2.0), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testBuyMarketOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        val limitOrder = buildMarketOrder(clientId = "Client1", volume = 100.0,
                fees = buildFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        size = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = match(limitOrder, getOrderBook("EURUSD", false))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client1", "EUR", BigDecimal.valueOf(99.0), BigDecimal.ZERO),
                        WalletOperation( "Client1", "USD", BigDecimal.valueOf(-120.0), BigDecimal.ZERO),
                        WalletOperation( "Client3", "EUR", BigDecimal.valueOf(1.0), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation( "Client2", "EUR", BigDecimal.valueOf(-100.0), BigDecimal.valueOf(-100.0)),
                        WalletOperation( "Client2", "USD", BigDecimal.valueOf(117.6), BigDecimal.ZERO),
                        WalletOperation( "Client4", "USD", BigDecimal.valueOf(2.4), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

}