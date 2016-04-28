package com.lykke.matching.engine.daos.bitcoin;

import com.google.gson.Gson;
import com.microsoft.azure.storage.table.TableServiceEntity;

import java.util.Date;

public class BtTransaction extends TableServiceEntity {

    public static String TRANS_ID = "TransId";

    String contextData;
    Date created = new Date();
    String requestData;

    public BtTransaction(String rowKey, Date created, String requestData, ClientCashOperationPair clientCashOperationPair) {
        this(rowKey, created, requestData, clientCashOperationPair, null);
    }

    public BtTransaction(String rowKey, Date created, String requestData, Orders orders) {
        this(rowKey, created, requestData, null, orders);
    }

    public BtTransaction(String rowKey, Date created, String requestData, ClientCashOperationPair clientCashOperationPair, Orders orders) {
        super(TRANS_ID, rowKey);
        this.created = created;
        this.requestData = requestData;

        if (clientCashOperationPair != null) {
            setClientCashOperationPair(clientCashOperationPair);
        }
        if (orders != null) {
            setOrders(orders);
        }
    }

    public void setOrders(Orders orders) {
        this.contextData = new Gson().toJson(orders);
    }

    public void setClientCashOperationPair(ClientCashOperationPair clientCashOperationPair) {
        this.contextData = new Gson().toJson(clientCashOperationPair);
    }

    public String getContextData() {
        return contextData;
    }

    public void setContextData(String contextData) {
        this.contextData = contextData;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getRequestData() {
        return requestData;
    }

    public void setRequestData(String requestData) {
        this.requestData = requestData;
    }
}