package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class Asset extends TableServiceEntity {
    public static String ASSET = "Asset";

    String blockChainId;
    int accuracy;
    Double dustLimit;

    public Asset() {
    }

    public Asset(String assetId, int accuracy) {
        this(assetId, accuracy, null);
    }

    public Asset(String assetId, int accuracy, Double dustLimit) {
        super(ASSET, assetId);
        this.accuracy = accuracy;
        this.dustLimit = dustLimit;
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

    public Double getDustLimit() {
        return dustLimit;
    }

    public void setDustLimit(Double dustLimit) {
        this.dustLimit = dustLimit;
    }

    @Override
    public String toString() {
        return "Asset{assetId=" + getAssetId() + "," +
                "blockChainId='" + blockChainId + '\'' +
                ", accuracy=" + accuracy +
                '}';
    }
}