package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class ExternalCashOperation extends TableServiceEntity {
    String cashOperationId;

    public ExternalCashOperation() {
    }

    public ExternalCashOperation(String clientId, String externalId, String cashOperationId) {
        super(clientId, externalId);
        this.cashOperationId = cashOperationId;
    }

    public String getCashOperationId() {
        return cashOperationId;
    }

    public void setCashOperationId(String cashOperationId) {
        this.cashOperationId = cashOperationId;
    }
}