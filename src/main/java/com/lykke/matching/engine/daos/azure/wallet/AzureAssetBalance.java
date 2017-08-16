package com.lykke.matching.engine.daos.azure.wallet;

public class AzureAssetBalance {
    String Asset;
    Double Balance = 0.0;
    Double Reserved = 0.0;

    AzureAssetBalance(String asset) {
        this.Asset = asset;
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

    public Double getReserved() {
        return Reserved;
    }

    public void setReserved(Double reserved) {
        Reserved = reserved;
    }
}
