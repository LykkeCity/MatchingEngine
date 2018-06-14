package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.WalletDatabaseAccessor

data class BalancesDatabaseAccessorsHolder(val primaryAccessor: WalletDatabaseAccessor,
                                           val secondaryAccessor: WalletDatabaseAccessor?)