package com.lykke.matching.engine.daos.azure.bitcoin;

public class AzureClientTradePair {
    private String ClientId;
    private String TradeId;

    public AzureClientTradePair(String clientId, String tradeId) {
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
