package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.AssetPair

class NewAssetPairsEvent(val assetPairs: List<AssetPair>)