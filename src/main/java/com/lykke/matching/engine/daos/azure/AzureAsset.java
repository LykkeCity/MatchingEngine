package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAsset extends TableServiceEntity {
    private static final String ASSET = "Asset";

    private String blockChainId;
    private int accuracy;

    public AzureAsset() {
    }

    public AzureAsset(String assetId, int accuracy) {
        super(ASSET, assetId);
        this.accuracy = accuracy;
    }

    public String getBlockChainId() {
        return blockChainId;
    }

    public void setBlockChainId(String blockChainId) {
        this.blockChainId = blockChainId;
    }

    public int getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(int accuracy) {
        this.accuracy = accuracy;
    }

    public String getAssetId() {
        return rowKey;
    }

    @Override
    public String toString() {
        return "AzureAsset{assetId='" + getAssetId() + '\'' +
                ", blockChainId='" + blockChainId + '\'' +
                ", accuracy=" + accuracy +
                '}';
    }
}