package com.lykke.matching.engine.daos.bitcoin;

public class Orders {
    ClientOrderPair MarketOrder;
    ClientOrderPair LimitOrder;
    ClientTradePair[] Trades;

    public Orders(ClientOrderPair marketOrder, ClientOrderPair limitOrder, ClientTradePair[] trades) {
        MarketOrder = marketOrder;
        LimitOrder = limitOrder;
        Trades = trades;
    }

    public ClientOrderPair getMarketOrder() {
        return MarketOrder;
    }

    public void setMarketOrder(ClientOrderPair marketOrder) {
        MarketOrder = marketOrder;
    }

    public ClientOrderPair getLimitOrder() {
        return LimitOrder;
    }

    public void setLimitOrder(ClientOrderPair limitOrder) {
        LimitOrder = limitOrder;
    }

    public ClientTradePair[] getTrades() {
        return Trades;
    }

    public void setTrades(ClientTradePair[] trades) {
        Trades = trades;
    }
}
