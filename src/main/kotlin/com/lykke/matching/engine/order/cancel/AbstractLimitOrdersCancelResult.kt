package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.utils.AbstractAssetOrderBook

abstract class AbstractLimitOrdersCancelResult<out T : AbstractAssetOrderBook>(val walletOperations: List<WalletOperation>,
                                                                               val clientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                                                               val trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>,
                                                                               val assetOrderBooks: Map<String, T>)