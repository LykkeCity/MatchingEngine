package com.lykke.matching.engine.daos.wallet;

public class AssetBalance {
    String Asset;
    Double Balance;

    public AssetBalance(String asset) {
        this(asset, 0.0);
    }

    public AssetBalance(String asset, Double balance) {
        Asset = asset;
        Balance = balance;
    }

    public String getAsset() {
        return Asset;
    }

    public void setAsset(String asset) {
        Asset = asset;
    }

    public Double getBalance() {
        return Balance;
    }

    public void setBalance(Double balance) {
        Balance = balance;
    }
}
