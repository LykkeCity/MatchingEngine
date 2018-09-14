package com.lykke.matching.engine.daos.azure.config;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureAppProperty extends TableServiceEntity {

    private String value;

    public AzureAppProperty() {
    }

    public AzureAppProperty(String partitionKey, String rowKey, String value) {
        super(partitionKey, rowKey);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}