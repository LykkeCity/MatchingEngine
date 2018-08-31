package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.getSetting
import org.junit.Ignore
import org.junit.Test
import java.math.BigDecimal
import java.util.*

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

        counter.executeAction { multiLimitOrderService.processMessage(MessageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                volumes = listOf(
                        VolumePrice(BigDecimal.valueOf(0.1), BigDecimal.valueOf(2.0)),
                        VolumePrice(BigDecimal.valueOf(0.1), BigDecimal.valueOf(1.5)),
                        VolumePrice(BigDecimal.valueOf(0.09),BigDecimal.valueOf( 1.3)),
                        VolumePrice(BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.2)),
                        VolumePrice(BigDecimal.valueOf(-1.0),BigDecimal.valueOf(2.1)),
                        VolumePrice(BigDecimal.valueOf(-0.09), BigDecimal.valueOf(2.2)),
                        VolumePrice(BigDecimal.valueOf(-0.1), BigDecimal.valueOf(2.4))
                ),
                ordersFee = listOf(),
                ordersFees = listOf()))
        }
        return counter.getAverageTime()
    }

    fun testAddLimitOrder(): Double {
        val counter = ActionTimeCounter()
        initServices()

        counter.executeAction {  multiLimitOrderService.processMessage(
                buildOldMultiLimitOrderWrapper(pair = "EURUSD",
                        clientId = "Client1",
                        volumes = listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.2)),
                                VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.3)))))}
        return counter.getAverageTime()
    }

    fun testAdd2LimitOrder(): Double {
        val counter = ActionTimeCounter()
        initServices()

        counter.executeAction {  multiLimitOrderService
                .processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.2)),
                        VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.3)))))}
        counter.executeAction { multiLimitOrderService
                .processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.4)),
                        VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.5))))) }

        return counter.getAverageTime()
    }

    fun testAddAndCancelLimitOrder(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction {  multiLimitOrderService
                .processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes =
                listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.2)), VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.3)))))}
        counter.executeAction {  multiLimitOrderService
                .processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes =
                listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.4)), VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.5)))))}
        counter.executeAction {  multiLimitOrderService
                .processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes =
                listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(2.0)), VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(2.1))), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.3)),
                VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.2))))) }

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.25, volume = -150.0)))

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(10.0), BigDecimal.valueOf(1.3)),
                        VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.26)),
                        VolumePrice(BigDecimal.valueOf(100.0), BigDecimal.valueOf(1.2))), cancel = true)) }

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder2(): Double {
        val counter = ActionTimeCounter()

        initServices()
        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(BigDecimal.valueOf(-100.0), BigDecimal.valueOf(1.2)),
                VolumePrice(BigDecimal.valueOf(-100.0), BigDecimal.valueOf(1.3))))) }


        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", price = 1.25, volume = 150.0)))

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-10.0), BigDecimal.valueOf(1.2)),
                        VolumePrice(BigDecimal.valueOf(-10.0), BigDecimal.valueOf(1.24)),
                        VolumePrice(BigDecimal.valueOf(-10.0), BigDecimal.valueOf(1.29)),
                        VolumePrice(BigDecimal.valueOf(-10.0), BigDecimal.valueOf(1.3))), cancel = true)) }


        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrder3(): Double {
        val counter = ActionTimeCounter()

        testBalanceHolderWrapper.updateBalance("Client5", "USD", 18.6)
        testBalanceHolderWrapper.updateBalance("Client5", "TIME", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "TIME", 1000.0)

        initServices()

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-100.0), BigDecimal.valueOf(26.955076))))) }
        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(0.69031943), BigDecimal.valueOf(26.915076)))))}

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "TIMEUSD", clientId = "Client2", price = 26.88023, volume = -26.0)))
        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(10.0), BigDecimal.valueOf(26.915076)), VolumePrice(BigDecimal.valueOf(10.0), BigDecimal.valueOf(26.875076))), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchLimitOrderZeroVolumes(): Double {
        val counter = ActionTimeCounter()

        testBalanceHolderWrapper.updateBalance("Client5", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(assetId = "BTCEUR", clientId = "Client2", price = 3629.355, volume = 0.19259621)))

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-0.00574996), BigDecimal.valueOf(3628.707))), cancel = true)) }
        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-0.01431186), BigDecimal.valueOf(3624.794)),
                        VolumePrice(BigDecimal.valueOf(-0.02956591), BigDecimal.valueOf(3626.591))), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-0.04996673), BigDecimal.valueOf(3625.855))), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-0.00628173), BigDecimal.valueOf(3622.865)),
                        VolumePrice(BigDecimal.valueOf(-0.01280207), BigDecimal.valueOf(3625.489)),
                        VolumePrice(BigDecimal.valueOf(-0.02201331), BigDecimal.valueOf(3627.41)),
                        VolumePrice(BigDecimal.valueOf(-0.02628901), BigDecimal.valueOf(3629.139))), cancel = true))}

        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-0.01708411), BigDecimal.valueOf(3626.11))), cancel = true))}
        counter.executeAction {multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-0.00959341), BigDecimal.valueOf(3625.302))), cancel = true))}

        return counter.getAverageTime()
    }

    fun testAddAndMatchAndCancel(): Double {
        val counter = ActionTimeCounter()

        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS.name, getSetting("Client3"))

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.26170853)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC",  0.001)
        testBalanceHolderWrapper.updateBalance("Client3", "CHF", 1000.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(MessageBuilder.buildLimitOrder(clientId = "Client2", assetId = "BTCCHF", uid = "1", price = 4384.15, volume = -0.26070853)))


        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3", volumes =
        listOf(VolumePrice(BigDecimal.valueOf(0.00643271), BigDecimal.valueOf(4390.84)),
                VolumePrice(BigDecimal.valueOf(0.01359005), BigDecimal.valueOf(4387.87)),
                VolumePrice(BigDecimal.valueOf(0.02033985), BigDecimal.valueOf(4384.811))), cancel = true)) }

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(0.01691068), BigDecimal.valueOf(4387.21))), cancel = true))}

        return counter.getAverageTime()
    }

    fun testMatchWithLimitOrderForAllFunds(): Double {
        val counter = ActionTimeCounter()

        val marketMaker = "Client1"
        val client = "Client2"

        testBalanceHolderWrapper.updateBalance(client, "EUR", 700.04)
        testBalanceHolderWrapper.updateBalance(client, "EUR", 700.04)
        testBalanceHolderWrapper.updateBalance(marketMaker, "BTC", 2.0)

        testOrderDatabaseAccessor.addLimitOrder(MessageBuilder.buildLimitOrder(clientId = client, assetId = "BTCEUR", price = 4722.0, volume = 0.14825226))
        initServices()

        counter.executeAction { multiLimitOrderService.processMessage(buildOldMultiLimitOrderWrapper(pair = "BTCEUR", clientId = marketMaker, volumes =
        listOf(VolumePrice(BigDecimal.valueOf(-0.4435), BigDecimal.valueOf(4721.403))), cancel = true)) }

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
        testOrderDatabaseAccessor.addLimitOrder(order)

        initServices()

        counter.executeAction { multiLimitOrderService.processMessage(MessageBuilder.buildMultiLimitOrderWrapper(clientId = marketMaker, pair = "EURUSD",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-2.0), BigDecimal.valueOf(1.1))), cancel = false, ordersFee = listOf(), ordersFees = listOf())) }

        return counter.getAverageTime()
    }

    fun testCancelPreviousOrderWithSameUid(): Double {
        val counter = ActionTimeCounter()

        counter.executeAction { multiLimitOrderService.processMessage(MessageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-9.0), BigDecimal.valueOf(0.4875))), ordersUid = listOf("order1"), cancel = true, ordersFee = emptyList(), ordersFees = emptyList())) }

        counter.executeAction { multiLimitOrderService.processMessage(MessageBuilder.buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                volumes = listOf(VolumePrice(BigDecimal.valueOf(-10.0), BigDecimal.valueOf(0.4880))), ordersUid = listOf("order1"), cancel = true, ordersFee = emptyList(), ordersFees = emptyList())) }

        return counter.getAverageTime()
    }


    private fun buildOldMultiLimitOrderWrapper(pair: String, clientId: String, volumes: List<VolumePrice>, cancel: Boolean = false): MessageWrapper {
        return MessageWrapper("Test", MessageType.OLD_MULTI_LIMIT_ORDER.type, buildOldMultiLimitOrder(pair, clientId, volumes, cancel).toByteArray(), null)
    }

    private fun buildOldMultiLimitOrder(assetPairId: String, clientId: String, volumes: List<VolumePrice>, cancel: Boolean): ProtocolMessages.OldMultiLimitOrder {
        val uid = Date().time
        val orderBuilder = ProtocolMessages.OldMultiLimitOrder.newBuilder()
                .setUid(uid)
                .setTimestamp(uid)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setCancelAllPreviousLimitOrders(cancel)
        volumes.forEach{ volume ->
            orderBuilder.addOrders(ProtocolMessages.OldMultiLimitOrder.Order.newBuilder()
                    .setVolume(volume.volume.toDouble())
                    .setPrice(volume.price.toDouble())
                    .build())
        }
        return orderBuilder.build()
    }
}