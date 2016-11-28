package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureWalletCredentials extends TableServiceEntity {
    private String multiSig;

    public AzureWalletCredentials() {
    }

    public AzureWalletCredentials(String partitionKey, String rowKey, String multiSig) {
        super(partitionKey, rowKey);
        this.multiSig = multiSig;
    }

    public String getMultiSig() {
        return multiSig;
    }

    public void setMultiSig(String multiSig) {
        this.multiSig = multiSig;
    }

    public String getClientId() {
        return rowKey;
    }
}