package com.lykke.matching.engine.daos.azure.bitcoin;

class AzureClientCashOperationPair {
    private String ClientId;
    private String CashOperationId;

    public AzureClientCashOperationPair(String clientId, String cashOperationId) {
        ClientId = clientId;
        CashOperationId = cashOperationId;
    }

    public String getClientId() {
        return ClientId;
    }

    public void setClientId(String clientId) {
        ClientId = clientId;
    }

    public String getCashOperationId() {
        return CashOperationId;
    }

    public void setCashOperationId(String cashOperationId) {
        CashOperationId = cashOperationId;
    }
}
