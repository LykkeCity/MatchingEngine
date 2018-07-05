package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import kotlin.test.assertNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (FeeTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FeeTest: AbstractTest() {

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        initServices()
    }

    @Test
    fun testBuyLimitOrderFeeOppositeAsset() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "BTC", balance = 0.1)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "USD", balance = 100.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client4", assetId = "USD", balance = 10.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client4", assetId = "BTC", balance = 0.1)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = -0.05,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.04),
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.05),
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!
                )
        ))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = 0.005,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = BigDecimal.valueOf(0.03),
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = BigDecimal.valueOf(0.02),
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!
                )
        )))

        assertEquals(BigDecimal.valueOf(75.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(0.0948), balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.00045), balancesHolder.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(3.75), balancesHolder.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(22.75), balancesHolder.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(0.005), balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(BigDecimal.valueOf(0.09975), balancesHolder.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(8.5), balancesHolder.getBalance("Client4", "USD"))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        val taker = event.orders.single { it.walletId == "Client2" }
        assertEquals(1, taker.trades?.size)
        assertEquals(2, taker.fees?.size)
        val takerTrade = taker.trades!!.first()
        assertEquals(2, takerTrade.fees?.size)

        val feeInstruction1 = taker.fees!!.single { it.size == "0.03" }
        val feeTransfer1 = takerTrade.fees!!.single { it.index == feeInstruction1.index }
        assertEquals("2.25", feeTransfer1.volume)
        assertEquals("USD", feeTransfer1.assetId)
        assertEquals("Client3", feeTransfer1.targetWalletId)
        val feeInstruction2 = taker.fees!!.single { it.size == "0.02" }
        val feeTransfer2 = takerTrade.fees!!.single { it.index == feeInstruction2.index }
        assertEquals("1.5", feeTransfer2.volume)
        assertEquals("USD", feeTransfer2.assetId)
        assertEquals("Client3", feeTransfer2.targetWalletId)
    }

    @Test
    fun testBuyLimitOrderFeeAnotherAsset() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))

        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "BTC", balance = 0.1)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "BTC", reservedBalance =  0.05)
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "EUR", balance = 25.0)

        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "USD", balance = 100.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "EUR", balance = 1.88)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "EURUSD", price = 1.3, volume = -1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "EURUSD", price = 1.1, volume = 1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCEUR", price = 13000.0, volume = -1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCEUR", price = 12000.0, volume = 1.0))


        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = -0.05,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.04),
                                targetClientId = "Client3",
                                assetIds = listOf("EUR"))!!
                )
        ))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = 0.005,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = BigDecimal.valueOf(0.03),
                                targetClientId = "Client3",
                                assetIds = listOf("EUR"))!!
                )
        )))

        assertEquals(BigDecimal.valueOf(75.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(0.095), balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(22.5), balancesHolder.getBalance("Client1", "EUR"))

        assertEquals(BigDecimal.valueOf(4.38), balancesHolder.getBalance("Client3", "EUR"))

        assertEquals(BigDecimal.valueOf(25.00), balancesHolder.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(0.005), balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client2", "EUR"))
    }

    @Test
    fun testSellMarketOrderFeeOppositeAsset() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 100.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.1)

        testBalanceHolderWrapper.updateBalance(clientId = "Client4", assetId = "USD", balance = 10.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client4", assetId = "BTC", balance = 0.1)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15154.123, volume = 0.005412,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.04),
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.05),
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!
                )
        ))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client2", assetId = "BTCUSD", volume = -0.005,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = BigDecimal.valueOf(0.03),
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = BigDecimal.valueOf(0.02),
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!
                )
        )))

        assertEquals(BigDecimal.valueOf(0.005), balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(21.19), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(75.77), balancesHolder.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(0.09485), balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(BigDecimal.valueOf(6.83), balancesHolder.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00025), balancesHolder.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.0999), balancesHolder.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(6.21), balancesHolder.getBalance("Client4", "USD"))
    }

    @Test
    fun testOrderBookNotEnoughFundsForFee() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 750.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.0503)

        initServices()

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$i", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = BigDecimal.valueOf(0.01),
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
            Thread.sleep(10)
        }

        assertEquals(5, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.05
        )))

        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order3" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order5" }.order.status)
        assertEquals(BigDecimal.valueOf(0.02), balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
    }

    @Test
    fun testOrderBookNotEnoughFundsForMultipleFee() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 600.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.0403)
        initServices()

        for (i in 1..2) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$i", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = BigDecimal.valueOf(0.01),
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
            Thread.sleep(10)
        }

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order3", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                fees = listOf(
                        buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.01),
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = BigDecimal.valueOf(0.01),
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!))))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order4", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                        makerSizeType = FeeSizeType.PERCENTAGE,
                        makerSize = BigDecimal.valueOf(0.01),
                        targetClientId = "Client3",
                        assetIds = listOf("BTC"))!!))))

        assertEquals(4, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.04
        )))

        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order3" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(BigDecimal.valueOf(0.01), balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee1() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 764.99)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.05)

        initServices()

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.05,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.02),
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.NotEnoughFunds.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee2() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 764.99)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.05)

        initServices()

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.05,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.02),
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.NotEnoughFunds.name, result.order.status)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee3() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 764.99)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.05)

        initServices()

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -750.0, straight = false,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.02),
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.NotEnoughFunds.name, result.order.status)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testNotEnoughFundsForFeeOppositeAsset() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 151.5)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.01521)

        initServices()

        val feeSizes = arrayListOf(0.01, 0.1, 0.01)
        feeSizes.forEachIndexed { index, feeSize ->
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$index", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.005,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = BigDecimal.valueOf(feeSize),
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
            Thread.sleep(10)
        }

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order4", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.02),
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.NotEnoughFunds.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(3, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order5", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.01),
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order0" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order5" }.order.status)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testNotEnoughFundsForFeeAnotherAsset() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))

        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "BTC", balance = 0.015)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "EUR", balance = 1.26)


        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 150.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "EUR", balance = 1.06)


        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "EURUSD", price = 1.3, volume = -1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "EURUSD", price = 1.1, volume = 1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCEUR", price = 11000.0, volume = -1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCEUR", price = 10000.0, volume = 1.0))

        initServices()

        val feeSizes = arrayListOf(0.01, 0.1, 0.01)
        feeSizes.forEachIndexed { index, feeSize ->
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$index", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.005,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = BigDecimal.valueOf(feeSize),
                            targetClientId = "Client3",
                            assetIds = listOf("EUR"))!!))))
            Thread.sleep(10)
        }

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order4", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.02),
                        targetClientId = "Client3",
                        assetIds = listOf("EUR"))!!))))

        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.NotEnoughFunds.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(3, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order5", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = BigDecimal.valueOf(0.01),
                        targetClientId = "Client3",
                        assetIds = listOf("EUR"))!!))))

        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order0" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order5" }.order.status)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testMakerFeeModificator() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "BTC", balance = 0.1)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "USD", balance = 100.0)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "AnotherClient", assetId = "BTCUSD", volume = -1.0, price = 10000.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "AnotherClient", assetId = "BTCUSD", volume = -1.0, price = 11000.0))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.01, price = 9700.0,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        makerSizeType = FeeSizeType.PERCENTAGE,
                        makerSize = BigDecimal.valueOf(0.04),
                        makerFeeModificator = BigDecimal.valueOf(50.0),
                        targetClientId = "TargetClient")!!)))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 9000.0,
                fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, takerSize = BigDecimal.valueOf(0.01), targetClientId = "TargetClient")!!))))

        // 0.01 * 0.04 * (1 - exp(-(10000.0 - 9700.0)/10000.0 * 50.0))
        assertEquals(BigDecimal.valueOf(0.00031075), balancesHolder.getBalance("TargetClient", "BTC"))

        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)

        assertEquals(1, result.orders.filter { it.order.clientId == "Client1" }.size)
        val takerResult = result.orders.first { it.order.clientId == "Client1" }
        assertEquals(1, takerResult.trades.size)
        assertEquals(BigDecimal.valueOf(300.0), takerResult.trades.first().absoluteSpread)
        assertEquals(BigDecimal.valueOf(0.03), takerResult.trades.first().relativeSpread)

        assertEquals(1, takerResult.trades.first().fees.size)
        assertNull(takerResult.trades.first().fees.first().transfer!!.feeCoef)

        assertEquals(1, result.orders.filter { it.order.clientId == "Client2" }.size)
        val makerResult = result.orders.first { it.order.clientId == "Client2" }
        assertEquals(1, makerResult.trades.size)
        assertEquals(BigDecimal.valueOf(300.0), makerResult.trades.first().absoluteSpread)
        assertEquals(BigDecimal.valueOf(0.03), makerResult.trades.first().relativeSpread)

        assertEquals(1, makerResult.trades.first().fees.size)
        assertEquals(BigDecimal.valueOf(0.776869839852), makerResult.trades.first().fees.first().transfer?.feeCoef)
    }

    @Test
    fun testMakerFeeModificatorForEmptyOppositeOrderBookSide() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "BTC", balance = 0.1)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "USD", balance = 100.0)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.01, price = 9700.0,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        makerSizeType = FeeSizeType.PERCENTAGE,
                        makerSize = BigDecimal.valueOf(0.04),
                        makerFeeModificator = BigDecimal.valueOf(50.0),
                        targetClientId = "TargetClient")!!)))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 9000.0,
                fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, takerSize = BigDecimal.valueOf(0.01), targetClientId = "TargetClient")!!))))

        assertEquals(BigDecimal.valueOf(0.0004), balancesHolder.getBalance("TargetClient", "BTC"))

        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)

        assertEquals(1, result.orders.filter { it.order.clientId == "Client1" }.size)
        val takerResult = result.orders.first { it.order.clientId == "Client1" }
        assertEquals(1, takerResult.trades.size)
        assertNull(takerResult.trades.first().absoluteSpread)
        assertNull(takerResult.trades.first().relativeSpread)

        assertEquals(1, takerResult.trades.first().fees.size)
        assertNull(takerResult.trades.first().fees.first().transfer!!.feeCoef)

        assertEquals(1, result.orders.filter { it.order.clientId == "Client2" }.size)
        val makerResult = result.orders.first { it.order.clientId == "Client2" }
        assertEquals(1, makerResult.trades.size)
        assertNull(makerResult.trades.first().absoluteSpread)
        assertNull(makerResult.trades.first().relativeSpread)

        assertEquals(1, makerResult.trades.first().fees.size)
        assertNull(makerResult.trades.first().fees.first().transfer!!.feeCoef)
    }

    private fun buildLimitOrderFeeInstruction(type: FeeType? = null,
                                              takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              takerSize: BigDecimal? = null,
                                              makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              makerSize: BigDecimal? = null,
                                              sourceClientId: String? = null,
                                              targetClientId: String? = null,
                                              assetIds: List<String> = listOf(),
                                              makerFeeModificator: BigDecimal? = null): NewLimitOrderFeeInstruction? {
        return if (type == null) null
        else return NewLimitOrderFeeInstruction(type, takerSizeType, takerSize, makerSizeType, makerSize, sourceClientId, targetClientId, assetIds, makerFeeModificator)
    }
}