package com.lykke.matching.engine.services

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class BalancesServiceTest {

    @Autowired
    private lateinit var balancesService: BalancesService

    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @Autowired
    private lateinit var testBalancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    @Autowired
    private lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

    @Test
    fun testInsertOrUpdateWallets() {
        balancesService.insertOrUpdateWallets(listOf(Wallet("test", listOf(AssetBalance("test", "BTC", BigDecimal.valueOf(10))))), null)


        assertEquals(BigDecimal.valueOf(10), balancesHolder.getBalance("test", "BTC"))
        assertEquals(BigDecimal.valueOf(10), testBalancesDatabaseAccessorsHolder.primaryAccessor.loadWallets()["test"]?.balances!!["BTC"]?.balance)
    }

    @Test
    fun testSendBalanceUpdate() {
        balancesService.sendBalanceUpdate(BalanceUpdate("test", "", Date(),
                listOf(ClientBalanceUpdate("test",
                        "BTC",
                        BigDecimal.valueOf(10),
                        BigDecimal.valueOf(15),
                        BigDecimal.valueOf(0),
                        BigDecimal.valueOf(1))),
                "testMessageId"))

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
    }
}