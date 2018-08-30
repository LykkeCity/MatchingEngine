package com.lykke.matching.engine.daos.azure.settings;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAppSetting extends TableServiceEntity {

    private String value;

    private String comment;

    private Boolean enabled;

    public AzureAppSetting() {
    }

    public AzureAppSetting(String partitionKey, String rowKey, String value, Boolean enabled, String comment) {
        super(partitionKey, rowKey);
        this.value = value;
        this.comment = comment;
        this.enabled = enabled;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }
}