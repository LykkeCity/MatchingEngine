package com.lykke.matching.engine.daos.wallet

import com.lykke.matching.engine.exception.BalanceException
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class WalletTest {

    @Test
    fun testOriginAndCopy() {
        val origin = Wallet("Client", listOf(
                AssetBalance("EUR", 1.0, 0.0),
                AssetBalance("USD", 2.0, 1.0),
                AssetBalance("BTC", 0.001, 0.0)
        ))

        val copy = origin.copy()

        assertEquals("Client", origin.clientId)
        assertEquals("Client", copy.clientId)

        copy.setReservedBalance("LKK", 1.5)
        copy.setBalance("EUR", 0.0)
        copy.setBalance("BTC", 0.002)

        assertEquals(3, origin.balances.size)
        assertEquals(1.0, origin.balances["EUR"]!!.balance)
        assertEquals(0.0, origin.balances["EUR"]!!.reserved)
        assertEquals(2.0, origin.balances["USD"]!!.balance)
        assertEquals(1.0, origin.balances["USD"]!!.reserved)
        assertEquals(0.001, origin.balances["BTC"]!!.balance)
        assertEquals(0.0, origin.balances["BTC"]!!.reserved)


        assertEquals(4, copy.balances.size)
        assertEquals(0.0, copy.balances["EUR"]!!.balance)
        assertEquals(0.0, copy.balances["EUR"]!!.reserved)
        assertEquals(2.0, copy.balances["USD"]!!.balance)
        assertEquals(1.0, copy.balances["USD"]!!.reserved)
        assertEquals(0.002, copy.balances["BTC"]!!.balance)
        assertEquals(0.0, copy.balances["BTC"]!!.reserved)
        assertEquals(1.5, copy.balances["LKK"]!!.balance)
        assertEquals(1.5, copy.balances["LKK"]!!.reserved)

        copy.applyToOrigin(origin)
        assertEquals(4, origin.balances.size)
        assertEquals(0.0, origin.balances["EUR"]!!.balance)
        assertEquals(0.0, origin.balances["EUR"]!!.reserved)
        assertEquals(2.0, origin.balances["USD"]!!.balance)
        assertEquals(1.0, origin.balances["USD"]!!.reserved)
        assertEquals(0.002, origin.balances["BTC"]!!.balance)
        assertEquals(0.0, origin.balances["BTC"]!!.reserved)
        assertEquals(1.5, origin.balances["LKK"]!!.balance)
        assertEquals(1.5, origin.balances["LKK"]!!.reserved)
    }

    @Test
    fun testValidateWallet() {
        val wallet = Wallet("Client")
        wallet.setBalance("Asset1", 1.0)
        wallet.setReservedBalance("Asset1", 1.0)
        wallet.validate()

        wallet.setBalance("Asset2", -1.0)
        assertFailsWith(BalanceException::class) { wallet.validate() }

        wallet.setBalance("Asset2", 0.0)
        wallet.setReservedBalance("Asset2", -1.0)
        assertFailsWith(BalanceException::class) { wallet.validate() }

        wallet.setBalance("Asset2", 1.0)
        wallet.setReservedBalance("Asset2", 2.0)
        assertFailsWith(BalanceException::class) { wallet.validate() }
    }

    @Test
    fun testValidateBalance() {
        AssetBalance("Asset", 1.0, 1.0).validate()

        // balance is negative
        assertFailsWith(BalanceException::class) { AssetBalance("Asset", -1.0, -1.1).validate() }
        var origin = AssetBalance("Asset", -1.0, 0.0)
        assertFailsWith(BalanceException::class) { origin.validate() }
        var copy = origin.copy()
        copy.validate()
        copy.balance = -1.1
        copy.reserved = -0.1
        assertFailsWith(BalanceException::class) { copy.validate() }

        // reserved is negative
        origin = AssetBalance("Asset", 0.0, -1.0)
        assertFailsWith(BalanceException::class) { origin.validate() }
        copy = origin.copy()
        copy.validate()
        copy.reserved = -1.1
        assertFailsWith(BalanceException::class) { copy.validate() }
        copy.reserved = -0.9
        copy.validate()
        copy.balance = 0.2
        copy.validate()
        copy.balance = -0.1
        assertFailsWith(BalanceException::class) { copy.validate() }

        // reserved is greater than balance
        origin = AssetBalance("Asset", 1.0, 2.0)
        assertFailsWith(BalanceException::class) { origin.validate() }
        copy = origin.copy()
        copy.validate()
        copy.reserved = 2.1
        assertFailsWith(BalanceException::class) { copy.validate() }
        copy.reserved = 1.9
        copy.validate()
        copy.balance = -0.1
        copy.reserved = 0.9
        assertFailsWith(BalanceException::class) { copy.validate() }
    }
}