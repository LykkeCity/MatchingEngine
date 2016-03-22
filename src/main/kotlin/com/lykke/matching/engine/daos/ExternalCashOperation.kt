package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class ExternalCashOperation: TableServiceEntity {
    var cashOperationId: String? = null

    constructor()

    constructor(clientId: String, externalId: String, cashOperationId: String) : super(clientId, externalId) {
        this.cashOperationId = cashOperationId
    }
}