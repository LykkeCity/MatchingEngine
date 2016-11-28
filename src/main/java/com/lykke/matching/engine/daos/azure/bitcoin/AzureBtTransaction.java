package com.lykke.matching.engine.daos.azure.bitcoin;

import com.google.gson.Gson;
import com.lykke.matching.engine.daos.bitcoin.ClientCashOperationPair;
import com.lykke.matching.engine.daos.bitcoin.Orders;
import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureBtTransaction extends TableServiceEntity {

    private static final String TRANS_ID = "TransId";

    private String contextData;
    private Date created = new Date();
    private String requestData;

    public AzureBtTransaction() {
    }

    public AzureBtTransaction(String rowKey, Date created, String requestData, ClientCashOperationPair clientCashOperationPair, Orders orders) {
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