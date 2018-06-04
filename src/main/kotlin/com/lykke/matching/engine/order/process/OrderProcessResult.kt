package com.lykke.matching.engine.order.process

class OrderProcessResult(val success: Boolean,
                         val orders: Collection<ProcessedOrder>)