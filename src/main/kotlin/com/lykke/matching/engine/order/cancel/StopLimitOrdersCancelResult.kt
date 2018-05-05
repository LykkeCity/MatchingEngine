package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.AssetStopOrderBook

class StopLimitOrdersCancelResult(walletOperations: List<WalletOperation>,
                                  clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                  trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                  assetOrderBooks: Map<String, AssetStopOrderBook>) :
        AbstractLimitOrdersCancelResult<AssetStopOrderBook>(walletOperations, clientsOrdersWithTrades, trustedClientsOrdersWithTrades, assetOrderBooks)