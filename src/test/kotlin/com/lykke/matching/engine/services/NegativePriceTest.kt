package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class NegativePriceTest : AbstractTest() {

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client", "USD", 1.0, 0.0))
        initServices()
    }

    @Test
    fun testLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client", assetId = "EURUSD", price = -1.0, volume = 1.0)))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val result = clientsLimitOrdersQueue.poll() as LimitOrdersReport

        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InvalidPrice.name, result.orders.first().order.status)
    }

    @Test
    fun testMultiLimitOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD",
                "Client",
                listOf(
                        VolumePrice(1.0, 1.0),
                        VolumePrice(1.0, -1.0)
                ),
                emptyList(),
                emptyList(),
                listOf("order1", "order2")))

        assertEquals(1, trustedClientsLimitOrdersQueue.size)
        val result = trustedClientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders.first { it.order.externalId == "order1" }.order.status)

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)
    }
}