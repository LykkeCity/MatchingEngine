package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.PrintUtils
import org.junit.Ignore
import org.junit.Test
import java.math.BigDecimal

@Ignore
class MultiLimitOrderServicePerformanceTest: AbstractPerformanceTest() {

    override fun initServices() {
        super.initServices()

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("TIMEUSD", "TIME", "USD", 6))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 8))

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("TIME", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
    }

    @Test
    fun testPerformance() {
        val averageOrderProcessionTime = Runner.runTests(REPEAT_TIMES, ::testSmallVolume,
                ::testAddLimitOrder, ::testAdd2LimitOrder, ::testAddAndCancelLimitOrder,
                ::testAddAndMatchLimitOrder, ::testAddAndMatchLimitOrder2, ::testAddAndMatchLimitOrder3,
                ::testAddAndMatchAndCancel, ::testAddAndMatchLimitOrderZeroVolumes, ::testMatchWithLimitOrderForAllFunds,
                ::testCancelPreviousOrderWithSameUid, ::testMatchWithNotEnoughFundsOrder1)
        println("Multilimit order average processing time is:  ${PrintUtils.convertToString2(averageOrderProcessionTime)}")
    }

    fun testSmallVolume(): Double {
        val counter = ActionTimeCounter()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))

        initServices()

        counter.executeAction {
            multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                    orders = listOf(IncomingLimitOrder(0.1, 2.0),
                            IncomingLimitOrder(0.1, 1.5),
                            IncomingLimitOrder(0.09, 1.3),
                            IncomingLimitOrder(1.0, 1.2),
                            IncomingLimitOrder(-1.0, 2.1),
                            IncomingLimitOrder(-0.09, 2.2),
                            IncomingLimitOrder(-0.1, 2.4)),
                    cancel = false))
        }
        return counter.getAverageTime()
    }

    fun testAddLimitOrder(): Double {
        val counter = ActionTimeCounter()
        initServices()

        counter.executeAction {  multiLimitOrderService.processMessage(
                messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD",
                        clientId = "Client1",
                        orders = listOf(IncomingLimitOrder(100.0, 1.2),
                                IncomingLimitOrder(100.0, 1.3)),
                        cancel = false))}
        return counter.getAverageTime()
    }

    fun testAdd2LimitOrder(): Double {
        val counter = ActionTimeCounter()
        initServices()

        counter.executeAction {
            multiLimitOrderService
                    .processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders = listOf(IncomingLimitOrder(100.0, 1.2),
                            IncomingLimitOrder(100.0, 1.3)),
                            cancel = false))
        }
        counter.executeAction {
            multiLimitOrderService
                    .processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders = listOf(IncomingLimitOrder(100.0, 1.4),
                            IncomingLimitOrder(100.0, 1.5)),
                            cancel = false))
        }

        return counter.getAverageTime()
    }

    fun testAddAndCancelLimitOrder(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction {  multiLimitOrderService
                .processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
                listOf(IncomingLimitOrder(100.0, 1.2), IncomingLimitOrder(100.0, 1.3)),
                        cancel = false))}
        counter.executeAction {  multiLimitOrderService
                .processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
                listOf(IncomingLimitOrder(100.0, 1.4), IncomingLimitOrder(100.0, 1.5)),
                        cancel = false))}
        counter.executeAction {  multiLimitOrderService
                .processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
                listOf(IncomingLimitOrder(100.0, 2.0), IncomingLimitOrder(100.0, 2.1)), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders = listOf(IncomingLimitOrder(100.0, 1.3),
                IncomingLimitOrder(100.0, 1.2)),
                cancel = false)) }

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.25, volume = -150.0)))

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                orders = listOf(IncomingLimitOrder(10.0, 1.3),
                        IncomingLimitOrder(100.0, 1.26),
                        IncomingLimitOrder(100.0, 1.2)), cancel = true)) }

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder2(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders = listOf(IncomingLimitOrder(-100.0, 1.2),
                IncomingLimitOrder(-100.0, 1.3)),
                cancel = false)) }


        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.25, volume = 150.0)))

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                orders = listOf(IncomingLimitOrder(-10.0, 1.2),
                        IncomingLimitOrder(-10.0, 1.24),
                        IncomingLimitOrder(-10.0, 1.29),
                        IncomingLimitOrder(-10.0, 1.3)), cancel = true)) }


        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder3(): Double {
        val counter = ActionTimeCounter()

        initServices()
        testBalanceHolderWrapper.updateBalance("Client5", "USD", 18.6)
        testBalanceHolderWrapper.updateBalance("Client5", "TIME", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "TIME", 1000.0)

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-100.0, 26.955076)),
                cancel = false)) }
        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(0.69031943, 26.915076)),
                cancel = false))}

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "TIMEUSD", clientId = "Client2", price = 26.88023, volume = -26.0)))
        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5",
                orders = listOf(IncomingLimitOrder(10.0, 26.915076), IncomingLimitOrder(10.0, 26.875076)), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrderZeroVolumes(): Double {
        val counter = ActionTimeCounter()

        initServices()

        testBalanceHolderWrapper.updateBalance("Client5", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", clientId = "Client2", price = 3629.355, volume = 0.19259621)))

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.00574996, 3628.707)), cancel = true)) }
        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5",
                orders = listOf(IncomingLimitOrder(-0.01431186, 3624.794),
                        IncomingLimitOrder(-0.02956591, 3626.591)), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.04996673, 3625.855)), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5",
                orders = listOf(IncomingLimitOrder(-0.00628173, 3622.865),
                        IncomingLimitOrder(-0.01280207, 3625.489),
                        IncomingLimitOrder(-0.02201331, 3627.41),
                        IncomingLimitOrder(-0.02628901, 3629.139)), cancel = true))}

        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.01708411, 3626.11)), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.00959341, 3625.302)), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchAndCancel(): Double {
        val counter = ActionTimeCounter()

        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "Client3", "Client3", true)

        initServices()

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.26170853)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC",  0.001)
        testBalanceHolderWrapper.updateBalance("Client3", "CHF", 1000.0)


        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", assetId = "BTCCHF", uid = "1", price = 4384.15, volume = -0.26070853)))


        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3", orders =
        listOf(IncomingLimitOrder(0.00643271, 4390.84),
                IncomingLimitOrder(0.01359005, 4387.87),
                IncomingLimitOrder(0.02033985, 4384.811)), cancel = true)) }

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3",
                orders = listOf(IncomingLimitOrder(0.01691068, 4387.21)), cancel = true))}

        return counter.getAverageTime()
    }

    fun testMatchWithLimitOrderForAllFunds(): Double {
        val counter = ActionTimeCounter()

        val marketMaker = "Client1"
        val client = "Client2"

        testBalanceHolderWrapper.updateBalance(client, "EUR", 700.04)
        testBalanceHolderWrapper.updateBalance(client, "EUR", 700.04)
        testBalanceHolderWrapper.updateBalance(marketMaker, "BTC", 2.0)

        primaryOrdersDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = client, assetId = "BTCEUR", price = 4722.0, volume = 0.14825226))
        initServices()

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = marketMaker, orders =
        listOf(IncomingLimitOrder(-0.4435, 4721.403)), cancel = true)) }

        return counter.getAverageTime()
    }


    fun testMatchWithNotEnoughFundsOrder1(): Double {
        val counter = ActionTimeCounter()

        val marketMaker = "Client1"
        val client = "Client2"
        testBalanceHolderWrapper.updateBalance(client, "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance(client, "USD", 1.19)

        val order = MessageBuilder.buildLimitOrder(clientId = client, assetId = "EURUSD", price = 1.2, volume = 1.0)
        order.reservedLimitVolume = BigDecimal.valueOf(1.19)
        primaryOrdersDatabaseAccessor.addLimitOrder(order)

        initServices()

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(clientId = marketMaker, pair = "EURUSD",
                orders = listOf(IncomingLimitOrder(-2.0, 1.1)), cancel = false)) }

        return counter.getAverageTime()
    }

    fun testCancelPreviousOrderWithSameUid(): Double {
        val counter = ActionTimeCounter()

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                orders = listOf(IncomingLimitOrder(-9.0, 0.4875, uid = "order1")), cancel = true)) }

        counter.executeAction { multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                orders = listOf(IncomingLimitOrder(-10.0, 0.4880, uid = "order1")), cancel = true)) }

        return counter.getAverageTime()
    }

}