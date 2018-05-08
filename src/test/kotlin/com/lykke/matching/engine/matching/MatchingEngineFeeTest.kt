package com.lykke.matching.engine.matching

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.buildWallet
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

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MatchingEngineTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MatchingEngineFeeTest : MatchingEngineTest() {

    @Test
    fun testSellLimitOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 121.12)
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.21111, volume = 100.0,
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

        initService()

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

        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -100.0, 0.0),
                        WalletOperation("", null, "Client2", "USD", now, 119.89, 0.0),
                        WalletOperation("", null, "Client3", "USD", now, 1.22, 0.0, true)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 97.8888, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -121.11, -121.11),
                        WalletOperation("", null, "Client4", "EUR", now, 2.1112, 0.0, true)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testBuyLimitOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 100.0)
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        initService()

        val limitOrder = buildLimitOrder(clientId = "Client1", price = 1.2, volume = 200.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        takerSize = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 99.0, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -120.0, 0.0),
                        WalletOperation("", null, "Client3", "EUR", now, 1.0, 0.0, true)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -100.0, -100.0),
                        WalletOperation("", null, "Client2", "USD", now, 117.6, 0.0),
                        WalletOperation("", null, "Client4", "USD", now, 2.4, 0.0, true)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testSellMarketOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 120.0)
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = 100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        initService()

        val limitOrder = buildMarketOrder(clientId = "Client2", volume = -100.0,
                fees = buildFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        size = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -100.0, 0.0),
                        WalletOperation("", null, "Client2", "USD", now, 118.8, 0.0),
                        WalletOperation("", null, "Client3", "USD", now, 1.2, 0.0, true)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 98.0, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -120.0, -120.0),
                        WalletOperation("", null, "Client4", "EUR", now, 2.0, 0.0, true)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testBuyMarketOrderFee() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 100.0)
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0,
                fees = buildLimitOrderFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        makerSize = 0.02,
                        targetClientId = "Client4"
                )
        ))

        initService()

        val limitOrder = buildMarketOrder(clientId = "Client1", volume = 100.0,
                fees = buildFeeInstructions(
                        type = FeeType.CLIENT_FEE,
                        size = 0.01,
                        targetClientId = "Client3"
                )
        )

        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 99.0, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -120.0, 0.0),
                        WalletOperation("", null, "Client3", "EUR", now, 1.0, 0.0, true)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -100.0, -100.0),
                        WalletOperation("", null, "Client2", "USD", now, 117.6, 0.0),
                        WalletOperation("", null, "Client4", "USD", now, 2.4, 0.0, true)
                ),
                matchingResult.oppositeCashMovements
        )
    }

}