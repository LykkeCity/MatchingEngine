package com.lykke.matching.engine.daos

data class Settings(val trustedClients: Set<String> = HashSet(),
val disabledAssets: Set<String> = HashSet())