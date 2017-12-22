package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.NewLimitOrder

class MatchedLimitOrderWrapper(val limitOrder: NewLimitOrder) {
    var status = limitOrder.status
    var lastMatchTime = limitOrder.lastMatchTime
    var remainingVolume = limitOrder.remainingVolume
    var reservedLimitVolume = limitOrder.reservedLimitVolume

    fun applyChanges() {
        limitOrder.status = status
        limitOrder.lastMatchTime = lastMatchTime
        limitOrder.remainingVolume = remainingVolume
        limitOrder.reservedLimitVolume = reservedLimitVolume
    }
}