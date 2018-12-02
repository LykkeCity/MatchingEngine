
package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAssetPair extends TableServiceEntity {

    private static final String ASSET_PAIR = "AssetPair";

    private String baseAssetId;
    private String quotingAssetId;
    private int accuracy;
    private Double minVolume;
    private Double minInvertedVolume;
    private Double maxVolume;
    private Double maxValue;
    private Double midPriceDeviationThreshold;
    private Double marketOrderPriceDeviationThreshold;

    public AzureAssetPair() {
    }

    public AzureAssetPair(String baseAssetId,
                          String quotingAssetId,
                          int accuracy,
                          Double minVolume,
                          Double minInvertedVolume,
                          Double maxVolume,
                          Double maxValue,
                          Double midPriceDeviationThreshold,
                          Double marketOrderPriceDeviationThreshold) {
        super(ASSET_PAIR, baseAssetId + quotingAssetId);
        this.baseAssetId = baseAssetId;
        this.quotingAssetId = quotingAssetId;
        this.accuracy = accuracy;
        this.minVolume = minVolume;
        this.minInvertedVolume = minInvertedVolume;
        this.maxVolume = maxVolume;
        this.maxValue = maxValue;
        this.midPriceDeviationThreshold = midPriceDeviationThreshold;
        this.marketOrderPriceDeviationThreshold = marketOrderPriceDeviationThreshold;
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

    public Double getMaxVolume() {
        return maxVolume;
    }

    public void setMaxVolume(Double maxVolume) {
        this.maxVolume = maxVolume;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Double maxValue) {
        this.maxValue = maxValue;
    }

    public Double getMidPriceDeviationThreshold() {
        return midPriceDeviationThreshold;
    }

    public void setMidPriceDeviationThreshold(Double midPriceDeviationThreshold) {
        this.midPriceDeviationThreshold = midPriceDeviationThreshold;
    }

    public Double getMarketOrderPriceDeviationThreshold() {
        return marketOrderPriceDeviationThreshold;
    }

    public void setMarketOrderPriceDeviationThreshold(Double marketOrderPriceDeviationThreshold) {
        this.marketOrderPriceDeviationThreshold = marketOrderPriceDeviationThreshold;
    }

    @Override
    public String toString() {
        return "AssetPair(pairId=" + getAssetPairId() +
                ", baseAssetId=" + baseAssetId +
                ", quotingAssetId=" + quotingAssetId +
                ", accuracy=" + accuracy +
                ", minVolume=" + minVolume +
                ", minInvertedVolume=" + minInvertedVolume +
                ", maxVolume=" + maxVolume +
                ", maxValue=" + maxValue +
                ", midPriceDeviationThreshold=" + midPriceDeviationThreshold +
                ", marketOrderPriceDeviationThreshold=" + marketOrderPriceDeviationThreshold +
                ")";
    }
}