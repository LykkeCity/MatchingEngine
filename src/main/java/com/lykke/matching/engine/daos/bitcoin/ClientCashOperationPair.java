package com.lykke.matching.engine.daos.bitcoin;

public class ClientCashOperationPair {
    String ClientId;
    String CashOperationId;

    public ClientCashOperationPair(String clientId, String cashOperationId) {
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
