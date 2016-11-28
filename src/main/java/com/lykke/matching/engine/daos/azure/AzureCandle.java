package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureCandle extends TableServiceEntity {
    private String data;

    public AzureCandle() {
    }

    public AzureCandle(String partitionKey, String rowKey, String data) {
        super(partitionKey, rowKey);
        this.data = data;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}