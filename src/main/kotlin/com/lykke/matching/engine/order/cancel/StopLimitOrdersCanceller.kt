package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.services.AssetStopOrderBook
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

class StopLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                               assetsHolder: AssetsHolder,
                               assetsPairsHolder: AssetsPairsHolder,
                               balancesHolder: BalancesHolder,
                               genericStopLimitOrderService: GenericStopLimitOrderService,
                               clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                               trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                               date: Date) :
        AbstractLimitOrdersCanceller<AssetStopOrderBook, StopLimitOrdersCancelResult>(dictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericStopLimitOrderService,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                date) {

    override fun processChangedOrderBook(orderBookCopy: AssetStopOrderBook, isBuy: Boolean) {
        // nothing to do
    }

    override fun getOrderLimitVolume(order: LimitOrder, limitAsset: Asset): BigDecimal {
        return order.reservedLimitVolume ?: if (order.isBuySide())
            NumberUtils.setScale(order.volume * (order.upperPrice ?: order.lowerPrice)!!, limitAsset.accuracy, false)
        else
            order.getAbsRemainingVolume()
    }

    override fun getCancelResult(walletOperations: List<WalletOperation>, clientsOrdersWithTrades: List<LimitOrderWithTrades>, trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>, assetOrderBooks: Map<String, AssetStopOrderBook>): StopLimitOrdersCancelResult {
        return StopLimitOrdersCancelResult(walletOperations, clientsOrdersWithTrades, trustedClientsOrdersWithTrades, assetOrderBooks)
    }
}