package com.lykke.matching.engine.daos.bitcoin;

public class ClientOrderPair {
    String ClientId;
    String OrderId;

    public ClientOrderPair(String clientId, String orderId) {
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
