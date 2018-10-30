package com.lykke.matching.engine.daos.azure.settings;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAppSetting extends TableServiceEntity {

    private String value;

    private Boolean enabled;

    public AzureAppSetting() {
    }

    public AzureAppSetting(String partitionKey, String rowKey, String value, Boolean enabled) {
        super(partitionKey, rowKey);
        this.value = value;
        this.enabled = enabled;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }
}