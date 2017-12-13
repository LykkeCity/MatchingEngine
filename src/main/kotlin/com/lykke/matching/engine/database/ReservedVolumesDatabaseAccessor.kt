package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection

interface ReservedVolumesDatabaseAccessor {
    fun addCorrectionsInfo(corrections: List<ReservedVolumeCorrection>)
}