package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.holders.BalancesHolder
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CurrentTransactionBalancesHolderTest {

    @Autowired
    private lateinit var balanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @Test
    fun testOneSubTransaction() {
        //given
        balanceHolderWrapper.updateBalance("Client", "BTC", 1.0)
        balanceHolderWrapper.updateBalance("Client", "USD", 1.0)
        balanceHolderWrapper.updateBalance("Client", "EUR", 1.0)

        //when
        val firstTransactionBalancesHolder = CurrentTransactionBalancesHolder(balancesHolder)
        firstTransactionBalancesHolder.updateBalance("Client", "BTC", BigDecimal.valueOf(0.9))

        val secondTransactionBalancesHolder = CurrentTransactionBalancesHolder(firstTransactionBalancesHolder)
        secondTransactionBalancesHolder.updateBalance("Client", "USD", BigDecimal.valueOf(0.8))

        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "EUR"))

        //then
        secondTransactionBalancesHolder.apply()
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "EUR"))

        val clientWallet = firstTransactionBalancesHolder.getWallet("Client")
        assertEquals(BigDecimal.valueOf(0.9), clientWallet!!.balances["BTC"]?.balance)
        assertEquals(BigDecimal.valueOf(0.8), clientWallet.balances["USD"]?.balance)
        assertEquals(BigDecimal.valueOf(1.0), clientWallet.balances["EUR"]?.balance)


        val persistenceData = secondTransactionBalancesHolder.persistenceData()
        assertEquals(2, persistenceData.balances.size)
        assertEquals(1, persistenceData.wallets.size)
        assertEquals(BigDecimal.valueOf(0.9), persistenceData.balances.findLast { it.asset == "BTC" }!!.balance)
        assertEquals(BigDecimal.valueOf(0.8), persistenceData.balances.findLast { it.asset == "USD" }!!.balance)

        firstTransactionBalancesHolder.apply()
        assertEquals(BigDecimal.valueOf(0.9), balancesHolder.getBalance("Client", "BTC"))
        assertEquals(BigDecimal.valueOf(0.8), balancesHolder.getBalance("Client", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getBalance("Client", "EUR"))
    }
}