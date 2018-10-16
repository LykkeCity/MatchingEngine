package com.lykke.matching.engine.web.dto

import com.lykke.matching.engine.daos.LimitOrder

data class ClientOrdersDto(val limitOrders: List<LimitOrder>, val stopOrders: List<LimitOrder>)