package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection

class TestReservedVolumesDatabaseAccessor : ReservedVolumesDatabaseAccessor {
    val corrections = ArrayList<ReservedVolumeCorrection>()

    override fun addCorrectionsInfo(corrections: List<ReservedVolumeCorrection>) {
        this.corrections.addAll(corrections)
    }
}