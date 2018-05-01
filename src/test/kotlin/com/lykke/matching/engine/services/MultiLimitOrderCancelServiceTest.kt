package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.UUID
import kotlin.test.assertEquals

class MultiLimitOrderCancelServiceTest : AbstractTest() {

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testSettingsDatabaseAccessor.addTrustedClient("TrustedClient")

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0, 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("TrustedClient", "BTC", 1.0))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.4, price = 10000.0, reservedVolume = 0.4))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.6, price = 11000.0, reservedVolume = 0.6))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.3, price = 10500.0))
        val partiallyMatchedTrustedClientOrder = buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.7, price = 11500.0)
        partiallyMatchedTrustedClientOrder.remainingVolume = -0.6
        testOrderDatabaseAccessor.addLimitOrder(partiallyMatchedTrustedClientOrder)

        initServices()
    }

    @Test
    fun testCancelTrustedClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("TrustedClient", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("TrustedClient", "BTC", 1.0, 0.0)
        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(1, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
        assertEquals(1, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, (trustedClientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)

        assertEquals(0, balanceUpdateQueue.size)
        assertEquals(1, tradesInfoQueue.size)
        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
    }

    @Test
    fun testCancelClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("Client1", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(2, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateQueue.size)
        assertEquals(1, tradesInfoQueue.size)
        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
    }

    private fun buildMultiLimitOrderCancelWrapper(clientId: String, assetPairId: String, isBuy: Boolean): MessageWrapper {
        return MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER_CANCEL.type, ProtocolMessages.MultiLimitOrderCancel.newBuilder()
                .setUid(UUID.randomUUID().toString())
                .setTimestamp(Date().time)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setIsBuy(isBuy).build().toByteArray(), null)
    }
}