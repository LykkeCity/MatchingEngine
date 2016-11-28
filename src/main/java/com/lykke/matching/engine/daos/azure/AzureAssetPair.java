package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAssetPair extends TableServiceEntity {

    private static final String ASSET_PAIR = "AssetPair";

    private String baseAssetId;
    private String quotingAssetId;
    private int accuracy;
    private int invertedAccuracy;

    public AzureAssetPair() {
    }

    public AzureAssetPair(String baseAssetId, String quotingAssetId, int accuracy, int invertedAccuracy) {
        super(ASSET_PAIR, baseAssetId + quotingAssetId);
        this.baseAssetId = baseAssetId;
        this.quotingAssetId = quotingAssetId;
        this.accuracy = accuracy;
        this.invertedAccuracy = invertedAccuracy;
    }

    public String getBaseAssetId() {
        return baseAssetId;
    }

    public void setBaseAssetId(String baseAssetId) {
        this.baseAssetId = baseAssetId;
    }

    public String getQuotingAssetId() {
        return quotingAssetId;
    }

    public void setQuotingAssetId(String quotingAssetId) {
        this.quotingAssetId = quotingAssetId;
    }

    public String getAssetPairId() {
        return rowKey;
    }

    public int getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(int accuracy) {
        this.accuracy = accuracy;
    }

    public int getInvertedAccuracy() {
        return invertedAccuracy;
    }

    public void setInvertedAccuracy(int invertedAccuracy) {
        this.invertedAccuracy = invertedAccuracy;
    }

    @Override
    public String toString() {
        return "AssetPair(pairId=" + getAssetPairId() + ",baseAssetId=" + baseAssetId + ", quotingAssetId=" + quotingAssetId + ", accuracy=" + accuracy + ", invertedAccuracy=" + invertedAccuracy + ")";
    }
}