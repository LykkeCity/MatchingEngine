package com.lykke.matching.engine.daos.wallet

import java.io.Serializable
import java.util.Date

class AssetBalance(val asset: String, val timestamp: Date, var balance: Double = 0.0, var reserved: Double = 0.0): Serializable
