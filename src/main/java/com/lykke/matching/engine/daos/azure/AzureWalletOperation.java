package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureWalletOperation extends TableServiceEntity {
    //partition key: Client Id
    //row key: id

    private String externalId;
    private String assetId;
    private Date dateTime = new Date();
    private Double amount;
    private String transactionId;

    public AzureWalletOperation() {
    }

    public AzureWalletOperation(String id, String externalId, String clientId, String assetId, Date dateTime, Double amount) {
        this(id, externalId, clientId, assetId, dateTime, amount, null);
    }

    public AzureWalletOperation(String id, String externalId, String clientId, String assetId, Date dateTime, Double amount, String transactionId) {
        super(clientId, id);
        this.dateTime = dateTime;
        this.assetId = assetId;
        this.amount = amount;
        this.transactionId = transactionId;
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

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @Override
    public String toString() {
        return "WalletOperation(" +
                "clientId=" + partitionKey +
                "id=" + rowKey +
                "externalId=" + externalId +
                "dateTime=" + dateTime +
                ", assetId='" + assetId + '\'' +
                ", amount=" + amount +
                ", transactionId='" + transactionId + '\'' +
                ')';
    }
}