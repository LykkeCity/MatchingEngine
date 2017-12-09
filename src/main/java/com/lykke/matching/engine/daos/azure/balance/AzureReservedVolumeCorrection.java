package com.lykke.matching.engine.daos.azure.balance;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AzureReservedVolumeCorrection extends TableServiceEntity {

    private static final String ROW_KEY_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private String clientId;
    private String assetId;
    private String orderIds;
    private Double oldReserved;
    private Double newReserved;

    public AzureReservedVolumeCorrection() {
    }

    public AzureReservedVolumeCorrection(Date date, String clientId, String assetId, String orderIds, Double oldReserved, Double newReserved) {
        super("ReservedVolumesCorrection", new SimpleDateFormat(ROW_KEY_DATE_FORMAT).format(date) + "_" + clientId + "_" + assetId);
        this.clientId = clientId;
        this.assetId = assetId;
        this.orderIds = orderIds;
        this.oldReserved = oldReserved;
        this.newReserved = newReserved;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getOrderIds() {
        return orderIds;
    }

    public void setOrderIds(String orderIds) {
        this.orderIds = orderIds;
    }

    public Double getOldReserved() {
        return oldReserved;
    }

    public void setOldReserved(Double oldReserved) {
        this.oldReserved = oldReserved;
    }

    public Double getNewReserved() {
        return newReserved;
    }

    public void setNewReserved(Double newReserved) {
        this.newReserved = newReserved;
    }

    @Override
    public String toString() {
        return "AzureReservedVolumeCorrection(" +
                "clientId=" + clientId +
                "orderIds=" + orderIds +
                "oldReserved=" + oldReserved +
                "newReserved=" + newReserved +
                ')';
    }
}
