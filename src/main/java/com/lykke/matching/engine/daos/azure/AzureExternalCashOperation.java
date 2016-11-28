package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureExternalCashOperation extends TableServiceEntity {
    private String cashOperationId;

    public AzureExternalCashOperation() {
    }

    public AzureExternalCashOperation(String clientId, String externalId, String cashOperationId) {
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