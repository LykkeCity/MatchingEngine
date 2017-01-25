package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureWalletSwapOperation extends TableServiceEntity {
    //partition key: id
    //row key: externalId

    private String externalId;
    private String clientId1;
    private String assetId1;
    private Double amount1;
    private String clientId2;
    private String assetId2;
    private Double amount2;
    private Date dateTime = new Date();

    public AzureWalletSwapOperation() {
    }

    public AzureWalletSwapOperation(String id, String externalId, String clientId1, String assetId1, Double amount1, String clientId2, String assetId2, Double amount2, Date dateTime) {
        super(id, externalId);
        this.externalId = externalId;
        this.clientId1 = clientId1;
        this.assetId1 = assetId1;
        this.amount1 = amount1;
        this.clientId2 = clientId2;
        this.assetId2 = assetId2;
        this.amount2 = amount2;
        this.dateTime = dateTime;
    }

    public String getClientId() {
        return partitionKey;
    }

    public String getId() {
        return rowKey;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getClientId1() {
        return clientId1;
    }

    public void setClientId1(String clientId1) {
        this.clientId1 = clientId1;
    }

    public String getAssetId1() {
        return assetId1;
    }

    public void setAssetId1(String assetId1) {
        this.assetId1 = assetId1;
    }

    public Double getAmount1() {
        return amount1;
    }

    public void setAmount1(Double amount1) {
        this.amount1 = amount1;
    }

    public String getClientId2() {
        return clientId2;
    }

    public void setClientId2(String clientId2) {
        this.clientId2 = clientId2;
    }

    public String getAssetId2() {
        return assetId2;
    }

    public void setAssetId2(String assetId2) {
        this.assetId2 = assetId2;
    }

    public Double getAmount2() {
        return amount2;
    }

    public void setAmount2(Double amount2) {
        this.amount2 = amount2;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }


    @Override
    public String toString() {
        return "AzureWalletSwapOperation{" +
                "id='" + rowKey + '\'' +
                ", externalId='" + externalId + '\'' +
                ", clientId1='" + clientId1 + '\'' +
                ", assetId1='" + assetId1 + '\'' +
                ", amount1=" + amount1 +
                ", clientId2='" + clientId2 + '\'' +
                ", assetId2='" + assetId2 + '\'' +
                ", amount2=" + amount2 +
                ", dateTime=" + dateTime +
                '}';
    }
}