package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.OrderBookDatabaseAccessor

class OrdersDatabaseAccessorsHolder(val primaryAccessor: OrderBookDatabaseAccessor,
                                    val secondaryAccessor: OrderBookDatabaseAccessor?)