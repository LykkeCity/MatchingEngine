package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class WalletCredentials extends TableServiceEntity {
    String multiSig;

    public WalletCredentials() {
    }

    public WalletCredentials(String partitionKey, String rowKey, String multiSig) {
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