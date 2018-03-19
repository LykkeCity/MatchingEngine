package com.lykke.matching.engine.utils.config

data class Settings(val trustedClients: Set<String>? = HashSet(),
                    val disabledAssets: Set<String>? = HashSet())