package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureMatchingData extends TableServiceEntity {
    //partition key: master order id
    //row key: matched order id

    private Double volume = 0.0;

    public AzureMatchingData() {
    }

    public AzureMatchingData(String masterOrderId, String matchedOrderId, Double volume) {
        super(masterOrderId, matchedOrderId);
        this.volume = volume;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }
}