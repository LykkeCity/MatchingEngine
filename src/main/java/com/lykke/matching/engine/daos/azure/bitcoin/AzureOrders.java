package com.lykke.matching.engine.daos.azure.bitcoin;

class AzureOrders {
    private AzureClientOrderPair MarketOrder;
    private AzureClientOrderPair LimitOrder;
    private AzureClientTradePair[] Trades;

    public AzureOrders(AzureClientOrderPair marketOrder, AzureClientOrderPair limitOrder, AzureClientTradePair[] trades) {
        MarketOrder = marketOrder;
        LimitOrder = limitOrder;
        Trades = trades;
    }

    public AzureClientOrderPair getMarketOrder() {
        return MarketOrder;
    }

    public void setMarketOrder(AzureClientOrderPair marketOrder) {
        MarketOrder = marketOrder;
    }

    public AzureClientOrderPair getLimitOrder() {
        return LimitOrder;
    }

    public void setLimitOrder(AzureClientOrderPair limitOrder) {
        LimitOrder = limitOrder;
    }

    public AzureClientTradePair[] getTrades() {
        return Trades;
    }

    public void setTrades(AzureClientTradePair[] trades) {
        Trades = trades;
    }
}
