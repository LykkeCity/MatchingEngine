package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureWalletTransferOperation extends TableServiceEntity {
    //partition key: uid
    //row key: assetId

    private String assetId;
    private String fromClientId;
    private String toClientId;
    private Date dateTime = new Date();
    private Double amount;

    public AzureWalletTransferOperation() {
    }

    public AzureWalletTransferOperation(String fromClientId, String toClientId, String uid, String assetId, Date dateTime, Double amount) {
        super(uid, assetId);
        this.dateTime = dateTime;
        this.assetId = assetId;
        this.amount = amount;
        this.fromClientId = fromClientId;
        this.toClientId = toClientId;
    }

    public String getClientId() {
        return partitionKey;
    }

    public String getUid() {
        return rowKey;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getFromClientId() {
        return fromClientId;
    }

    public void setFromClientId(String fromClientId) {
        this.fromClientId = fromClientId;
    }

    public String getToClientId() {
        return toClientId;
    }

    public void setToClientId(String toClientId) {
        this.toClientId = toClientId;
    }

    @Override
    public String toString() {
        return "AzureWalletTransferOperation{" +
                "assetId='" + assetId + '\'' +
                ", fromClientId='" + fromClientId + '\'' +
                ", toClientId='" + toClientId + '\'' +
                ", dateTime=" + dateTime +
                ", amount=" + amount +
                '}';
    }
}