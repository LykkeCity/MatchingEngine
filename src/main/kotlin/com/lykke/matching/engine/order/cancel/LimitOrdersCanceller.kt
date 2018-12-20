package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.*

class LimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                           assetsHolder: AssetsHolder,
                           assetsPairsHolder: AssetsPairsHolder,
                           balancesHolder: BalancesHolder,
                           genericLimitOrderService: GenericLimitOrderService,
                           applicationSettingsHolder: ApplicationSettingsHolder,
                           date: Date) :
        AbstractLimitOrdersCanceller<AssetOrderBook, LimitOrdersCancelResult>(dictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsHolder,
                genericLimitOrderService,
                date) {


    override fun getCancelResult(walletOperations: List<WalletOperation>, clientsOrdersWithTrades: List<LimitOrderWithTrades>, trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>, assetOrderBooks: Map<String, AssetOrderBook>): LimitOrdersCancelResult {
        return LimitOrdersCancelResult(walletOperations,
                clientsOrdersWithTrades,
                trustedClientsOrdersWithTrades,
                assetOrderBooks)
    }

    override fun getOrderLimitVolume(order: LimitOrder, limitAsset: Asset): BigDecimal {
        return order.reservedLimitVolume ?: if (order.isBuySide())
            NumberUtils.setScale(order.getAbsRemainingVolume() * order.price, limitAsset.accuracy, false)
        else
            order.getAbsRemainingVolume()
    }


}