package com.lykke.matching.engine.daos.bitcoin;

public class ClientTradePair {
    String ClientId;
    String TradeId;

    public ClientTradePair(String clientId, String tradeId) {
        ClientId = clientId;
        TradeId = tradeId;
    }

    public String getClientId() {
        return ClientId;
    }

    public void setClientId(String clientId) {
        ClientId = clientId;
    }

    public String getTradeId() {
        return TradeId;
    }

    public void setTradeId(String tradeId) {
        TradeId = tradeId;
    }
}
