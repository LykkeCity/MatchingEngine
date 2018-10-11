package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor

class StopOrdersDatabaseAccessorsHolder(val primaryAccessor: StopOrderBookDatabaseAccessor,
                                        val secondaryAccessor: StopOrderBookDatabaseAccessor?)