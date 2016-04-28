package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AssetPair extends TableServiceEntity {

    public static String ASSET_PAIR = "AssetPair";

    String baseAssetId;
    String quotingAssetId;

    public AssetPair() {
    }

    public AssetPair(String baseAssetId, String quotingAssetId) {
        super(ASSET_PAIR, baseAssetId + quotingAssetId);
        this.baseAssetId = baseAssetId;
        this.quotingAssetId = quotingAssetId;
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

    @Override
    public String toString() {
        return "AssetPair(baseAssetId=" + baseAssetId + ", quotingAssetId=" + quotingAssetId + ")";
    }
}