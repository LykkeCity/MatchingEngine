package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class Asset: TableServiceEntity() {
    var blockChainId: String = ""
}