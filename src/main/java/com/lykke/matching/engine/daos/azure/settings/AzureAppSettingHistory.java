package com.lykke.matching.engine.daos.azure.settings;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.UUID;

public class AzureAppSettingHistory extends TableServiceEntity {
    private String settingName;

    private String value;

    private String comment;

    private String user;

    private Boolean enabled;

    public AzureAppSettingHistory() {
    }

    public AzureAppSettingHistory(String partitionKey, String settingName, String value, String comment, String user, Boolean enabled) {
        super(partitionKey, UUID.randomUUID().toString());
        this.settingName = settingName;
        this.value = value;
        this.comment = comment;
        this.user = user;
        this.enabled = enabled;
    }

    public String getSettingName() {
        return settingName;
    }

    public String getValue() {
        return value;
    }

    public String getComment() {
        return comment;
    }

    public String getUser() {
        return user;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public void setSettingName(String settingName) {
        this.settingName = settingName;
    }
}
