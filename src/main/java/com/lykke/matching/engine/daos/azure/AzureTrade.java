package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureTrade extends TableServiceEntity {
    //row key: generated externalId

    private String assetId;
    private Date dateTime = new Date();
    private String limitOrderId;
    private String marketOrderId;
    private Double volume;
    private Double price;

    private String addressFrom;
    private String addressTo;

    private String clientId;
    private String multisig;

    public static final String DATE_TIME = "dt";

    public AzureTrade() {
    }

    public AzureTrade(String partitionKey, String rowKey, String clientId, String multisig, String assetId, Date dateTime, String limitOrderId, String marketOrderId, Double volume, Double price, String addressFrom, String addressTo) {
        super(partitionKey, rowKey);
        this.clientId = clientId;
        this.multisig = multisig;
        this.assetId = assetId;
        this.dateTime = dateTime;
        this.limitOrderId = limitOrderId;
        this.marketOrderId = marketOrderId;
        this.volume = volume;
        this.price = price;
        this.addressFrom = addressFrom;
        this.addressTo = addressTo;
    }

    public String getClientId() {
        return clientId != null ? clientId : partitionKey;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getMultisig() {
        return multisig;
    }

    public void setMultisig(String multisig) {
        this.multisig = multisig;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getLimitOrderId() {
        return limitOrderId;
    }

    public void setLimitOrderId(String limitOrderId) {
        this.limitOrderId = limitOrderId;
    }

    public String getMarketOrderId() {
        return marketOrderId;
    }

    public void setMarketOrderId(String marketOrderId) {
        this.marketOrderId = marketOrderId;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getAddressFrom() {
        return addressFrom;
    }

    public void setAddressFrom(String addressFrom) {
        this.addressFrom = addressFrom;
    }

    public String getAddressTo() {
        return addressTo;
    }

    public void setAddressTo(String addressTo) {
        this.addressTo = addressTo;
    }

    @Override
    public String toString() {
        return "Trade(" +
                "clientId='" + getClientId() + '\'' +
                "multisig='" + multisig + '\'' +
                ", externalId='" + rowKey + '\'' +
                ", assetId='" + assetId + '\'' +
                ", dateTime=" + dateTime +
                ", limitOrderId='" + limitOrderId + '\'' +
                ", marketOrderId='" + marketOrderId + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ", addressFrom=" + addressFrom +
                ", addressTo=" + addressTo +
                ')';
    }

    public AzureTrade cloneWithGeneratedId() {
        return new AzureTrade(DATE_TIME, rowKey, clientId, multisig, assetId, dateTime, limitOrderId, marketOrderId, volume, price, addressFrom, addressTo);
    }

    public AzureTrade cloneWithMultisig() {
        return new AzureTrade(multisig, rowKey, clientId, multisig, assetId, dateTime, limitOrderId, marketOrderId, volume, price, addressFrom, addressTo);
    }
}