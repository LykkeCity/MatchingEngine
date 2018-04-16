package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades

data class OrdersWithTrades(val clientsOrders: List<LimitOrderWithTrades>,
                            val trustedClientsOrders: List<LimitOrderWithTrades>)