package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderSide.Sell
import com.lykke.matching.engine.order.OrderStatus
import org.junit.Before
import org.junit.Test
import java.util.Date
import kotlin.test.assertNotNull

class LimitOrderServiceTest {

    val testDatabaseAccessor = TestLimitOrderDatabaseAccessor()

    @Before
    fun setUp() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "1", price = 100.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "2", price = 200.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "3", price = 300.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "4", price = 400.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "5", price = 100.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "6", price = 200.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "7", price = 300.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "8", price = 400.0, orderType = Sell.name))
    }

    @Test
    fun testAddLimitOrder() {
        val service = LimitOrderService(testDatabaseAccessor)
        service.processMessage(buildByteArray(buildLimitOrder(price = 999.9)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)
    }

    private fun buildByteArray(order: LimitOrder): ByteArray {
        return ProtocolMessages.LimitOrder.newBuilder()
                .setUid(order.rowKey.toLong())
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetId(order.assetId)
                .setOrderAction(OrderSide.valueOf(order.orderType).side)
                .setBlockChain(order.blockChain)
                .setVolume(order.volume)
                .setPrice(order.price).build().toByteArray()
    }

    private fun buildLimitOrder(partitionKey: String = "EURUSD_Buy",
                                rowKey: String = "1",
                                assetId: String = "EURUSD",
                                clientId: String = "Client1",
                                executed: Date? = null,
                                isOrderTaken: String = "",
                                orderType: String = Buy.name,
                                price: Double = 100.0,
                                registered: Date = Date(),
                                status: String = OrderStatus.InOrderBook.name,
                                volume:Double = 1000.0): LimitOrder =
            LimitOrder(
                    partitionKey = partitionKey,
                    rowKey = rowKey,
                    assetId = assetId,
                    clientId = clientId,
                    executed = executed,
                    isOrderTaken = isOrderTaken,
                    orderType = orderType,
                    blockChain = "",
                    price = price,
                    createdAt = registered,
                    registered = Date(),
                    status = status,
                    volume = volume
            )
}
