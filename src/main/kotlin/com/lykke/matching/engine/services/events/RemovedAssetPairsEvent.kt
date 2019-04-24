package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.AssetPair

class RemovedAssetPairsEvent(val assetPairs: Collection<AssetPair>)