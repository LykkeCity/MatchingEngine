package com.lykke.matching.engine.holders

import com.lykke.matching.engine.balance.BalancesGetter
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class BalancesHolder(private val balancesDbAccessorsHolder: BalancesDatabaseAccessorsHolder): BalancesGetter {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(BalancesHolder::class.java.name)
    }

    lateinit var wallets: MutableMap<String, Wallet>
    var initialClientsCount = 0
    var initialBalancesCount = 0

    init {
        update()
    }

    private fun update() {
        wallets = balancesDbAccessorsHolder.primaryAccessor.loadWallets()
        initialClientsCount = wallets.size
        initialBalancesCount = wallets.values.sumBy { it.balances.size }
    }

    fun clientExists(clientId: String): Boolean {
        return wallets.containsKey(clientId)
    }

    fun getBalances(clientId: String): Map<String, AssetBalance> {
        val wallet = wallets[clientId]
        return wallet?.balances ?: emptyMap()
    }

    fun getBalance(clientId: String, assetId: String): BigDecimal {
        return getBalances(clientId)[assetId]?.balance ?: BigDecimal.ZERO
    }

    override fun getReservedBalance(clientId: String, assetId: String): BigDecimal {
        return getBalances(clientId)[assetId]?.reserved ?: BigDecimal.ZERO
    }

    override fun getAvailableBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return (if (balance.reserved > BigDecimal.ZERO)
                    balance.balance - balance.reserved
                else balance.balance)
            }
        }

        return BigDecimal.ZERO
    }

    override fun getAvailableReservedBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                // reserved can be greater than base balance due to transfer with overdraft
                return if (balance.reserved.signum() == 1 && balance.reserved <= balance.balance) balance.reserved else balance.balance
            }
        }

        return BigDecimal.ZERO
    }

    fun setWallets(wallets: Collection<Wallet>) {
        wallets.forEach { wallet ->
            this.wallets[wallet.clientId] = wallet
        }
    }
}