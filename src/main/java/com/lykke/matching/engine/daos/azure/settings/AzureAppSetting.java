package com.lykke.matching.engine.daos.azure.settings;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAppSetting extends TableServiceEntity {

    private String value;

    private String comment;

    private String user;

    private Boolean enabled;

    public AzureAppSetting() {
    }

    public AzureAppSetting(String partitionKey, String rowKey, String value, Boolean enabled, String comment, String user) {
        super(partitionKey, rowKey);
        this.value = value;
        this.comment = comment;
        this.enabled = enabled;
        this.user = user;
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}