package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.util.LinkedList;
import java.util.StringJoiner;

public class HourCandle extends TableServiceEntity {

    public static String MICRO = "micro";

    String data;

    public HourCandle() {
    }

    public HourCandle(String asset, String data) {
        super(MICRO, asset);
        this.data = data;
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

    public void addPrice(Double price) {
        LinkedList<Double> prices = getPricesList();
        prices.add(price);
        while (prices.size() < 20) {
            prices.add(price);
        }
        if (prices.size() > 20 ) {
            prices.removeFirst();
        }
        StringJoiner joiner = new StringJoiner(";");
        for (Double prc : prices) {
            joiner.add(String.valueOf(prc));
        }
        this.data = joiner.toString();
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