package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class ClientMultiLimitOrderTest : AbstractTest() {

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 3000.0))

        initServices()
    }

    @Test
    fun testAdd() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        VolumePrice(-0.1, 10000.0),
                        VolumePrice(-0.2, 10500.0),
                        VolumePrice(-0.30000001, 11000.0),
                        VolumePrice(0.1, 9500.0),
                        VolumePrice(0.2, 9000.0)
                ),
                emptyList(), emptyList(), listOf("1", "2", "3", "4", "5")))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, orderBookQueue.size)
        assertEquals(2, rabbitOrderBookQueue.size)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateQueue.size)

        assertEquals(2, tradesInfoQueue.size)

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(5, report.orders.size)
        report.orders.forEach {
            assertEquals(OrderStatus.InOrderBook.name, it.order.status)
        }
        assertEquals(0.1, report.orders.first { it.order.externalId == "1" }.order.reservedLimitVolume)
        assertEquals(0.2, report.orders.first { it.order.externalId == "2" }.order.reservedLimitVolume)
        assertEquals(0.30000001, report.orders.first { it.order.externalId == "3" }.order.reservedLimitVolume)
        assertEquals(950.0, report.orders.first { it.order.externalId == "4" }.order.reservedLimitVolume)
        assertEquals(1800.0, report.orders.first { it.order.externalId == "5" }.order.reservedLimitVolume)
    }

    @Test
    fun testAddOneSide() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        VolumePrice(-0.1, 10000.0),
                        VolumePrice(-0.2, 10500.0)
                ),
                emptyList(), emptyList()))

        assertOrderBookSize("BTCUSD", false, 2)
        assertOrderBookSize("BTCUSD", true, 0)

        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
        assertEquals(1, tradesInfoQueue.size)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateQueue.size)

        assertBalance("Client1", "BTC", 1.0, 0.3)
        assertBalance("Client1", "USD", 3000.0, 0.0)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
    }

    @Test
    fun testCancelAllPrevious() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.2))
        initServices()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10500.0, volume = -0.2)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-1", clientId = "Client1", assetId = "BTCUSD", price = 10100.0, volume = -0.4)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-2", clientId = "Client1", assetId = "BTCUSD", price = 11000.0, volume = -0.3)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-3", clientId = "Client1", assetId = "BTCUSD", price = 9000.0, volume = 0.1)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-4", clientId = "Client1", assetId = "BTCUSD", price = 8000.0, volume = 0.2)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-5", clientId = "Client1", assetId = "BTCUSD", price = 7000.0, volume = 0.001)))

        clearMessageQueues()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        VolumePrice(-0.1, 10000.0),
                        VolumePrice(-0.2, 10500.0),
                        VolumePrice(-0.30000001, 11000.0),
                        VolumePrice(0.1, 9500.0),
                        VolumePrice(0.2, 9000.0)
                ),
                emptyList(), emptyList(), cancel = true))

        assertOrderBookSize("BTCUSD", false, 4)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, orderBookQueue.size)
        assertEquals(2, rabbitOrderBookQueue.size)
        assertEquals(2, tradesInfoQueue.size)

        val buyOrderBook = orderBookQueue.first { it.isBuy }
        val sellOrderBook = orderBookQueue.first { !it.isBuy }
        assertEquals(2, buyOrderBook.prices.size)
        assertEquals(9500.0, buyOrderBook.prices.first().price)
        assertEquals(4, sellOrderBook.prices.size)
        assertEquals(10000.0, sellOrderBook.prices.first().price)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateQueue.size)

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)
        assertBalance("Client2", "BTC", 0.2, 0.2)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(10, report.orders.size)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2", "ForCancel-3", "ForCancel-4", "ForCancel-5"), cancelledIds)
    }

    @Test
    fun testCancelAllPreviousOneSide() {

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-1", clientId = "Client1", assetId = "BTCUSD", price = 10100.0, volume = -0.4)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-2", clientId = "Client1", assetId = "BTCUSD", price = 11000.0, volume = -0.3)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 9000.0, volume = 0.1)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 8000.0, volume = 0.2)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 7000.0, volume = 0.001)))

        clearMessageQueues()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        VolumePrice(-0.1, 10000.0),
                        VolumePrice(-0.2, 10500.0),
                        VolumePrice(-0.30000001, 11000.0)
                ),
                emptyList(), emptyList(), cancel = true))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 3)

        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
        assertEquals(1, tradesInfoQueue.size)

        val sellOrderBook = orderBookQueue.first { !it.isBuy }
        assertEquals(3, sellOrderBook.prices.size)
        assertEquals(10000.0, sellOrderBook.prices.first().price)

        assertBalance("Client1", "BTC", 1.0, 0.60000001)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(5, report.orders.size)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2"), cancelledIds)
    }

    @Test
    fun testAddNotEnoughFundsOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        VolumePrice(-0.1, 10000.0),
                        VolumePrice(-0.2, 10500.0),
                        VolumePrice(-0.30000001, 11000.0),
                        VolumePrice(-0.4, 12000.0),
                        VolumePrice(0.1, 9500.0),
                        VolumePrice(0.2, 9000.0),
                        VolumePrice(0.03, 9500.0)
                ),
                emptyList(), emptyList(), listOf("1", "2", "3", "ToReject-1", "5", "6", "ToReject-2")))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, orderBookQueue.size)
        assertEquals(2, rabbitOrderBookQueue.size)
        assertEquals(2, tradesInfoQueue.size)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateQueue.size)

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(7, report.orders.size)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first { it.order.externalId == "ToReject-1" }.order.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first { it.order.externalId == "ToReject-2" }.order.status)
    }

    @Test
    fun testMatch() {
        testSettingsDatabaseAccessor.addTrustedClient("TrustedClient")

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 10000.0))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 10000.0))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 0.2))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("TrustedClient", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("TrustedClient", "USD", 3000.0))

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "TrustedClient", listOf(
                VolumePrice(-0.3, 10800.0),
                VolumePrice(-0.4, 10900.0),
                VolumePrice(0.1, 9500.0),
                VolumePrice(0.2, 9300.0)
        ), emptyList(), emptyList(), listOf("3", "2", "6", "7")))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "ToCancelDueToNoFundsForFee", clientId = "Client3", assetId = "BTCUSD", volume = -0.2, price = 10500.0,
                fees = buildLimitOrderFeeInstructions(FeeType.CLIENT_FEE, makerSize = 0.05, targetClientId = "TargetClient", assetIds = listOf("BTC"))
        )))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client2", listOf(
                VolumePrice(-0.1, 10000.0),
                VolumePrice(-0.5, 11000.0),
                VolumePrice(0.3, 9000.0),
                VolumePrice(0.4, 8800.0)
        ), emptyList(), emptyList(), listOf("5", "1", "8", "9")))

        clearMessageQueues()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                VolumePrice(-0.1, 11500.0),
                VolumePrice(0.05, 11000.0),
                VolumePrice(0.2, 10800.0),
                VolumePrice(0.1, 9900.0)
        ), emptyList(), emptyList(), listOf("14", "12", "13", "11")))


        assertOrderBookSize("BTCUSD", false, 4)
        assertOrderBookSize("BTCUSD", true, 5)

        assertEquals(0, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(7, report.orders.size)

        val orderIds = report.orders.map { it.order.externalId }.toMutableList()
        orderIds.sort()
        assertEquals(listOf("11", "12", "13", "14", "3", "5", "ToCancelDueToNoFundsForFee"), orderIds)

        val matchedIds = report.orders.filter { it.order.status == OrderStatus.Matched.name }.map { it.order.externalId }.toMutableList()
        matchedIds.sort()
        assertEquals(listOf("12", "13", "5"), matchedIds)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ToCancelDueToNoFundsForFee"), cancelledIds)

        val addedIds = report.orders.filter { it.order.status == OrderStatus.InOrderBook.name }.map { it.order.externalId }.toMutableList()
        addedIds.sort()
        assertEquals(listOf("11", "14"), addedIds)

        val partiallyMatchedIds = report.orders.filter { it.order.status == OrderStatus.Processing.name }.map { it.order.externalId }.toMutableList()
        partiallyMatchedIds.sort()
        assertEquals(listOf("3"), partiallyMatchedIds)


        assertBalance("Client1", "BTC", 1.25, 0.1)
        assertBalance("Client1", "USD", 7380.0, 990.0)
        assertBalance("Client3", "BTC", 0.2, 0.0)

        assertBalance("Client2", "BTC", 0.9, 0.5)
        assertBalance("Client2", "USD", 11000.0, 6220.0)

        assertBalance("TrustedClient", "BTC", 0.85, 0.0)
        assertBalance("TrustedClient", "USD", 4620.0, 0.0)
    }

    @Test
    fun testNegativeSpread() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                VolumePrice(-0.1, 10000.0),
                VolumePrice(0.1, 10100.0)
        ), emptyList(), emptyList()))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)

        assertBalance("Client1", "BTC", 1.0, 0.1)
        assertBalance("Client1", "USD", 3000.0, 0.0)
    }

    @Test
    fun testCancelPreviousAndMatch() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2400.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 0.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.3, price = 9500.0)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                VolumePrice(0.1, 9000.0),
                VolumePrice(0.1, 8000.0),
                VolumePrice(0.1, 7000.0)
        ), emptyList(), emptyList()))

        clearMessageQueues()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                VolumePrice(0.1, 10000.0),
                VolumePrice(0.01, 9500.0),
                VolumePrice(0.1, 9000.0),
                VolumePrice(0.1, 8000.0)
        ), emptyList(), emptyList(), cancel = true))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 1)

        assertEquals(9000.0, genericLimitOrderService.getOrderBook("BTCUSD").getBidPrice())
        assertEquals(9500.0, genericLimitOrderService.getOrderBook("BTCUSD").getAskPrice())

        assertBalance("Client1", "USD", 1355.0, 900.0)
        assertBalance("Client1", "BTC", 0.11, 0.0)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(8, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
    }

}