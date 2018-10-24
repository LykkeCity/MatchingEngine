package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.daos.MidPrice

class MidPricePersistenceData(val midPrices: List<MidPrice>? = null,
                              val removeAll: Boolean = false) {
    constructor(midPrice: MidPrice? = null, removeAll: Boolean = false)
            : this(if(midPrice != null) listOf(midPrice) else null, removeAll)
}