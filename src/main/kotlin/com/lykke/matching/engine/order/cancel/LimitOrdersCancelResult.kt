package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook

class LimitOrdersCancelResult(walletOperations: List<WalletOperation>,
                              clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                              trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                              assetOrderBooks: Map<String, AssetOrderBook>,
                              val orderBooks: List<OrderBook>) :
        AbstractLimitOrdersCancelResult<AssetOrderBook>(walletOperations, clientsOrdersWithTrades, trustedClientsOrdersWithTrades, assetOrderBooks)