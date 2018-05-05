package com.lykke.matching.engine.order.cancel

data class GenericLimitOrdersCancelResult(val limitOrdersCancelResult: LimitOrdersCancelResult,
                                          val stopLimitOrdersCancelResult: StopLimitOrdersCancelResult)