package com.lykke.matching.engine.daos.wallet

import java.io.Serializable

class AssetBalance(val clientId: String,
                   val asset: String,
                   var balance: Double = 0.0,
                   var reserved: Double = 0.0) : Serializable