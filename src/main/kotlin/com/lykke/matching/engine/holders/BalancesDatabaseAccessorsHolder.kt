package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.utils.config.RedisConfig

data class BalancesDatabaseAccessorsHolder(val primaryAccessor: WalletDatabaseAccessor,
                                           val secondaryAccessor: WalletDatabaseAccessor?,
                                           val redisConfig: RedisConfig)