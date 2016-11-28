package com.lykke.matching.engine.queue

import com.google.gson.Gson
import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.daos.bitcoin.ClientOrderPair
import com.lykke.matching.engine.daos.bitcoin.ClientTradePair
import com.lykke.matching.engine.daos.bitcoin.Orders
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.queue.transaction.CashIn
import com.lykke.matching.engine.queue.transaction.CashOut
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class BackendQueueProcessorTest {

    val backOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val walletCredentialsCache = WalletCredentialsCache(backOfficeDatabaseAccessor)

    @Before
    fun setUp() {
        backOfficeDatabaseAccessor.clear()
        var credentials = WalletCredentials("Client1", "TestMultiSig1")
        backOfficeDatabaseAccessor.credentials.put("Client1", credentials)

        credentials = WalletCredentials("Client2", "TestMultiSig2")
        backOfficeDatabaseAccessor.credentials.put("Client2", credentials)

        var asset = Asset("USD", 2, "TestUSD")
        backOfficeDatabaseAccessor.assets.put("USD", asset)
        asset = Asset("EUR", 2, "TestEUR")
        backOfficeDatabaseAccessor.assets.put("EUR", asset)

        walletCredentialsCache.reloadCache()
    }

    @Test
    fun testCashIn() {
        val inQueue = LinkedBlockingQueue<Transaction>()
        val outQueueWriter = TestQueueWriter()
        val processor = BackendQueueProcessor(backOfficeDatabaseAccessor, inQueue, outQueueWriter, walletCredentialsCache)

        val cashIn = CashIn(TransactionId = "11", clientId = "Client1", Amount = 500.0, Currency = "USD", cashOperationId = "123")
        processor.processMessage(cashIn)

        val cashInData = Gson().fromJson(outQueueWriter.read().replace("CashIn:", ""), CashIn::class.java)

        assertEquals("TestMultiSig1",cashInData.MultisigAddress)
        assertEquals(500.0,cashInData.Amount)
        assertEquals("TestUSD",cashInData.Currency)

        val transaction = backOfficeDatabaseAccessor.transactions.find { it.id == cashInData.TransactionId }!!
        assertEquals("Client1", transaction.clientCashOperationPair!!.clientId)
        assertEquals("123", transaction.clientCashOperationPair!!.cashOperationId)
    }

    @Test
    fun testCashOut() {
        val inQueue = LinkedBlockingQueue<Transaction>()
        val outQueueWriter = TestQueueWriter()
        val processor = BackendQueueProcessor(backOfficeDatabaseAccessor, inQueue, outQueueWriter, walletCredentialsCache)

        val cashOut = CashOut(TransactionId = "11", clientId = "Client1", Amount = 500.0, Currency = "USD", cashOperationId = "123")
        processor.processMessage(cashOut)

        val cashOutData = Gson().fromJson(outQueueWriter.read().replace("CashOut:", ""), CashOut::class.java)

        assertEquals("TestMultiSig1",cashOutData.MultisigAddress)
        assertEquals(500.0,cashOutData.Amount)
        assertEquals("TestUSD",cashOutData.Currency)

        val transaction = backOfficeDatabaseAccessor.transactions.find { it.id == cashOutData.TransactionId }!!
        assertEquals("Client1", transaction.clientCashOperationPair!!.clientId)
        assertEquals("123", transaction.clientCashOperationPair!!.cashOperationId)
    }

    @Test
    fun testSwap() {
        val inQueue = LinkedBlockingQueue<Transaction>()
        val outQueueWriter = TestQueueWriter()
        val processor = BackendQueueProcessor(backOfficeDatabaseAccessor, inQueue, outQueueWriter, walletCredentialsCache)

        val swap = Swap(TransactionId = "11", clientId1 = "Client1", Amount1 = 500.0, origAsset1 = "USD",
                        clientId2 = "Client2", Amount2 = 500.0, origAsset2 = "EUR",
                        orders = Orders(ClientOrderPair("Client1", "Order1"), ClientOrderPair("Client2", "Order2"),
                                arrayOf(ClientTradePair("Client1", "uid1"), ClientTradePair("Client1", "uid2"),
                                        ClientTradePair("Client2", "uid3"), ClientTradePair("Client2", "uid4"))))
        processor.processMessage(swap)

        val swapData = Gson().fromJson(outQueueWriter.read().replace("Swap:", ""), Swap::class.java)

        assertEquals("TestMultiSig1",swap.MultisigCustomer1)
        assertEquals(500.0,swap.Amount1)
        assertEquals("TestUSD",swap.Asset1)
        assertEquals("TestMultiSig2",swap.MultisigCustomer2)
        assertEquals(500.0,swap.Amount2)
        assertEquals("TestEUR",swap.Asset2)


        val transaction = backOfficeDatabaseAccessor.transactions.find { it.id == swapData.TransactionId }!!
        assertEquals("Client1", transaction.orders!!.marketOrder.clientId)
        assertEquals("Order1", transaction.orders!!.marketOrder.orderId)
        assertEquals("Client2", transaction.orders!!.limitOrder.clientId)
        assertEquals("Order2", transaction.orders!!.limitOrder.orderId)
        assertNotNull(transaction.orders!!.trades.find { it.clientId == "Client1" && it.tradeId == "uid1" })
        assertNotNull(transaction.orders!!.trades.find { it.clientId == "Client1" && it.tradeId == "uid2" })
        assertNotNull(transaction.orders!!.trades.find { it.clientId == "Client2" && it.tradeId == "uid3" })
        assertNotNull(transaction.orders!!.trades.find { it.clientId == "Client2" && it.tradeId == "uid4" })
    }
}