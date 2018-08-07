package com.lykke.matching.engine.daos.context

data class LimitOrderCancelOperationContext(val limitOrderIds: Set<String>,
                                            val uid: String)