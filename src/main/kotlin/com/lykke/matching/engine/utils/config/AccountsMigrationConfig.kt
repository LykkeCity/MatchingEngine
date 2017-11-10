package com.lykke.matching.engine.utils.config

data class AccountsMigrationConfig(
        /** 0 - disabled, 1 - from db to files, 2 - from files to db */
        val mode: Int,
        val accountsTableName: String
) {
    companion object {
        const val MODE_FROM_DB_TO_FILES = 1
        const val MODE_FROM_FILES_TO_DB = 2
    }
}