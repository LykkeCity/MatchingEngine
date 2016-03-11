package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class WalletCredentials : TableServiceEntity() {
    var privateKey = ""
    var multiSig = ""
}