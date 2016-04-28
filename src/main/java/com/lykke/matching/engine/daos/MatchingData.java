package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class MatchingData extends TableServiceEntity {
    //partition key: master order id
    //row key: matched order id

    Double volume = 0.0;

    public MatchingData() {
    }

    public MatchingData(String masterOrderId, String matchedOrderId, Double volume) {
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