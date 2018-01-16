package com.lykke.matching.engine.daos.azure.wallet;

import com.google.gson.Gson;
import com.lykke.matching.engine.daos.wallet.AssetBalance;
import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class AzureWallet extends TableServiceEntity {
    //partition key: "ClientBalance"
    //row key: clientId

    private static final String CLIENT_BALANCE = "ClientBalance";

    private String balances;

    public AzureWallet() {
    }

    public AzureWallet(String clientId) {
        super(CLIENT_BALANCE, clientId);
    }

    public AzureWallet(String clientId, List<AssetBalance> balances) {
        super(CLIENT_BALANCE, clientId);
        if (balances.isEmpty()) {
            this.balances = "[]";
        } else {
            balances.forEach(balance -> addBalance(balance.getAsset(), balance.getBalance(), balance.getReserved()));
        }
    }

    public String getClientId() {
        return rowKey;
    }

    public String getBalances() {
        return balances;
    }

    public void setBalances(String balances) {
        this.balances = balances;
    }

    public void addBalance(String asset, Double amount, Double reserved) {
        List<AzureAssetBalance> assetBalances = getBalancesList();
        Optional<AzureAssetBalance> assetBalanceFilter = assetBalances.stream().filter(it -> asset.equals(it.Asset) ).findFirst();
        AzureAssetBalance assetBalance;
        if (assetBalanceFilter.isPresent()) {
            assetBalance = assetBalanceFilter.get();
        } else {
            assetBalance = new AzureAssetBalance(asset);
            assetBalances.add(assetBalance);
        }

        assetBalance.Balance += amount;
        assetBalance.Reserved += reserved;
        if (assetBalance.Balance == 0.0) {
            assetBalances.remove(assetBalance);
        }

        this.balances = new Gson().toJson(assetBalances);
    }

    public void setBalance(String asset, Double amount, Double reserved) {
        List<AzureAssetBalance> assetBalances = getBalancesList();
        Optional<AzureAssetBalance> assetBalanceFilter = assetBalances.stream().filter(it -> asset.equals(it.Asset) ).findFirst();
        AzureAssetBalance assetBalance;
        if (assetBalanceFilter.isPresent()) {
            assetBalance = assetBalanceFilter.get();
        } else {
            assetBalance = new AzureAssetBalance(asset);
            assetBalances.add(assetBalance);
        }

        assetBalance.Balance = amount;
        assetBalance.Reserved = reserved;

        this.balances = new Gson().toJson(assetBalances);
    }

    public Double getBalance(String asset) {
        return getAssetBalance(asset).Balance;
    }

    private AzureAssetBalance getAssetBalance(String asset) {
        if (balances != null) {
            List<AzureAssetBalance> assetBalances = getBalancesList();
            Optional<AzureAssetBalance> result = assetBalances.stream().filter(it -> asset.equals(it.Asset) ).findFirst();
            return result.isPresent() ? result.get() : new AzureAssetBalance(asset);
        }

        return new AzureAssetBalance(asset);
    }

    public List<AzureAssetBalance> getBalancesList() {
        List<AzureAssetBalance> result = new ArrayList<>();
        if (balances != null) {
            result.addAll(Arrays.asList((AzureAssetBalance[]) (new Gson().fromJson(balances, AzureAssetBalance[].class))));
        }
        return result;
    }
}