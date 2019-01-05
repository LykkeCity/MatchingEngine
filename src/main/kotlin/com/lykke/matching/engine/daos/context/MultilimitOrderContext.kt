package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult

class MultilimitOrderContext(val assetPair: AssetPair?,
                             val baseAsset: Asset?,
                             val quotingAsset: Asset?,
                             val clientId: String,
                             val isTrustedClient: Boolean,
                             val multiLimitOrder: MultiLimitOrder,
                             var inputValidationResultByOrderId: Map<String, OrderValidationResult>? = null)