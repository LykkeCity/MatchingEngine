package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class Candle extends TableServiceEntity {
    String data;

    public Candle() {
    }

    public Candle(String partitionKey, String rowKey, String data) {
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