package com.lykke.matching.engine.daos.azure.bitcoin;

public class AzureClientOrderPair {
    private String ClientId;
    private String OrderId;

    public AzureClientOrderPair(String clientId, String orderId) {
        ClientId = clientId;
        OrderId = orderId;
    }

    public String getClientId() {
        return ClientId;
    }

    public void setClientId(String clientId) {
        ClientId = clientId;
    }

    public String getOrderId() {
        return OrderId;
    }

    public void setOrderId(String orderId) {
        OrderId = orderId;
    }
}
