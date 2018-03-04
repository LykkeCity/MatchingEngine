package com.lykke.matching.engine.daos.azure.settings;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class DisabledAsset extends TableServiceEntity {

    public static final String DISABLED_ASSET = "DisabledAsset";

    private Boolean isDisabled;

    public DisabledAsset() {
    }

    public DisabledAsset(String assetId, Boolean isDisabled) {
        super(DISABLED_ASSET, assetId);
        this.isDisabled = isDisabled;
    }

    public String getAsset() {
        return rowKey;
    }

    public Boolean getDisabled() {
        return isDisabled;
    }

    public void setDisabled(Boolean disabled) {
        isDisabled = disabled;
    }

    @Override
    public String toString() {
        return "DisabledAsset{" +
                "asset='" + rowKey + '\'' +
                ", isDisabled=" + isDisabled +
                '}';
    }
}
