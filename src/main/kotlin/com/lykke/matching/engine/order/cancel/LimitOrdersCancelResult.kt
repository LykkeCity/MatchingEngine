package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook

data class LimitOrdersCancelResult(val walletOperations: List<WalletOperation>,
                                   val clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                   val trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                   val assetOrderBooks: Map<String, AssetOrderBook>,
                                   val orderBooks: List<OrderBook>)