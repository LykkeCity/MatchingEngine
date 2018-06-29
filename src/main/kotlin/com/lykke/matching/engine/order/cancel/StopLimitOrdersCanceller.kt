package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.AssetStopOrderBook
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

class StopLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                               assetsPairsHolder: AssetsPairsHolder,
                               balancesHolder: BalancesHolder,
                               genericStopLimitOrderService: GenericStopLimitOrderService,
                               clientLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                               trustedClientsLimitOrderQueue: BlockingQueue<JsonSerializable>,
                               date: Date) :
        AbstractLimitOrdersCanceller<AssetStopOrderBook, StopLimitOrdersCancelResult>(dictionariesDatabaseAccessor,
                assetsPairsHolder,
                balancesHolder,
                genericStopLimitOrderService,
                clientLimitOrdersQueue,
                trustedClientsLimitOrderQueue,
                date) {

    override fun processChangedOrderBook(orderBookCopy: AssetStopOrderBook, isBuy: Boolean) {
        // nothing to do
    }

    override fun getOrderLimitVolume(order: LimitOrder): BigDecimal {
        return order.reservedLimitVolume ?: BigDecimal.ZERO
    }

    override fun getCancelResult(walletOperations: List<WalletOperation>, clientsOrdersWithTrades: List<LimitOrderWithTrades>, trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>, assetOrderBooks: Map<String, AssetStopOrderBook>): StopLimitOrdersCancelResult {
        return StopLimitOrdersCancelResult(walletOperations, clientsOrdersWithTrades, trustedClientsOrdersWithTrades, assetOrderBooks)
    }
}