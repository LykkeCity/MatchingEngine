package com.lykke.matching.engine.holders

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class OrderBookMaxTotalSizeHolderImpl(@Value("#{Config.me.orderBookMaxTotalSize}")
                                      private val orderBookMaxTotalSize: Int?) : OrderBookMaxTotalSizeHolder {
    override fun get() = orderBookMaxTotalSize
}