package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAssetPair extends TableServiceEntity {

    private static final String ASSET_PAIR = "AssetPair";

    private String baseAssetId;
    private String quotingAssetId;
    private int accuracy;
    private Double minVolume;
    private Double minInvertedVolume;

    public AzureAssetPair() {
    }

    public AzureAssetPair(String baseAssetId, String quotingAssetId, int accuracy, Double minVolume, Double minInvertedVolume) {
        super(ASSET_PAIR, baseAssetId + quotingAssetId);
        this.baseAssetId = baseAssetId;
        this.quotingAssetId = quotingAssetId;
        this.accuracy = accuracy;
        this.minVolume = minVolume;
        this.minInvertedVolume = minInvertedVolume;
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

    public Double getMinVolume() {
        return minVolume;
    }

    public void setMinVolume(Double minVolume) {
        this.minVolume = minVolume;
    }

    public Double getMinInvertedVolume() {
        return minInvertedVolume;
    }

    public void setMinInvertedVolume(Double minInvertedVolume) {
        this.minInvertedVolume = minInvertedVolume;
    }

    @Override
    public String toString() {
        return "AssetPair(pairId=" + getAssetPairId() + ",baseAssetId=" + baseAssetId + ", quotingAssetId=" + quotingAssetId + ", accuracy=" + accuracy + ", minVolume=" + minVolume + ", minInvertedVolume=" + minInvertedVolume + ")";
    }
}