package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.LinkedList;
import java.util.StringJoiner;

public class AzureHourCandle extends TableServiceEntity {

    public static final String MICRO = "micro";

    private String data;

    public AzureHourCandle() {
    }

    public AzureHourCandle(String asset, LinkedList<Double> prices) {
        super(MICRO, asset);

        StringJoiner joiner = new StringJoiner(";");
        for (Double prc : prices) {
            joiner.add(String.valueOf(prc));
        }
        this.data = joiner.toString();
    }

    public String getAsset() {
        return rowKey;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public LinkedList<Double> getPricesList() {
        LinkedList<Double> result = new LinkedList<>();
        if (data != null) {
            for ( String price : data.split(";")) {
                result.add(Double.valueOf(price));
            }
        }
        return result;
    }
}