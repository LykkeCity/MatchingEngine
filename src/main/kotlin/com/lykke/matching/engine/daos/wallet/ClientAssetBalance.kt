package com.lykke.matching.engine.daos.wallet

import java.io.Serializable

class ClientAssetBalance(val clientId: String,
                         val assetBalance: AssetBalance) : Serializable