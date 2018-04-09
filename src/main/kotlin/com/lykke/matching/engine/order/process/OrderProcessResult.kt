package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.NewLimitOrder

class OrderProcessResult(val acceptedOrders: Collection<NewLimitOrder>,
                         val rejectedOrders: Collection<NewLimitOrder>)