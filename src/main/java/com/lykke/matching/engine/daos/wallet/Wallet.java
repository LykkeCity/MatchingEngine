package com.lykke.matching.engine.daos.wallet;

import com.google.gson.Gson;
import com.microsoft.azure.storage.table.TableServiceEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Wallet extends TableServiceEntity {
    //partition key: "ClientBalance"
    //row key: clientId

    public static String CLIENT_BALANCE = "ClientBalance";

    String balances;

    public Wallet() {
    }

    public Wallet(String rowKey) {
        super(CLIENT_BALANCE, rowKey);
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

    public void addBalance(String asset, Double amount) {
        List<AssetBalance> assetBalances = getBalancesList();
        Optional<AssetBalance> assetBalanceFilter = assetBalances.stream().filter( it -> asset.equals(it.Asset) ).findFirst();
        AssetBalance assetBalance;
        if (assetBalanceFilter.isPresent()) {
            assetBalance = assetBalanceFilter.get();
        } else {
            assetBalance = new AssetBalance(asset);
            assetBalances.add(assetBalance);
        }

        assetBalance.Balance += amount;
        if (assetBalance.Balance == 0.0) {
            assetBalances.remove(assetBalance);
        }

        this.balances = new Gson().toJson(assetBalances);
    }

    public void setBalance(String asset, Double amount) {
        List<AssetBalance> assetBalances = getBalancesList();
        Optional<AssetBalance> assetBalanceFilter = assetBalances.stream().filter( it -> asset.equals(it.Asset) ).findFirst();
        AssetBalance assetBalance;
        if (assetBalanceFilter.isPresent()) {
            assetBalance = assetBalanceFilter.get();
        } else {
            assetBalance = new AssetBalance(asset);
            assetBalances.add(assetBalance);
        }

        assetBalance.Balance = amount;

        this.balances = new Gson().toJson(assetBalances);
    }

    public Double getBalance(String asset) {
        return getAssetBalance(asset).Balance;
    }

    private AssetBalance getAssetBalance(String asset) {
        if (balances != null) {
            List<AssetBalance> assetBalances = getBalancesList();
            Optional<AssetBalance> result = assetBalances.stream().filter( it -> asset.equals(it.Asset) ).findFirst();
            return result.isPresent() ? result.get() : new AssetBalance(asset, 0.0);
        }

        return new AssetBalance(asset, 0.0);
    }

    public List<AssetBalance> getBalancesList() {
        List<AssetBalance> result = new ArrayList<>();
        if (balances != null) {
            result.addAll(Arrays.asList((AssetBalance[]) (new Gson().fromJson(balances, AssetBalance[].class))));
        }
        return result;
    }
}