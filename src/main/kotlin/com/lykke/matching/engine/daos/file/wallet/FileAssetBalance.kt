package com.lykke.matching.engine.daos.file.wallet

import java.io.Serializable
import java.util.Date

class FileAssetBalance(val asset: String, val timestamp: Date, var balance: Double = 0.0, var reserved: Double = 0.0): Serializable