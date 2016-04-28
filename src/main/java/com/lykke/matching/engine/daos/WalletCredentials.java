package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class WalletCredentials extends TableServiceEntity {
    String privateKey;
    String multiSig;

    public WalletCredentials() {
    }

    public WalletCredentials(String partitionKey, String rowKey, String privateKey, String multiSig) {
        super(partitionKey, rowKey);
        this.privateKey = privateKey;
        this.multiSig = multiSig;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getMultiSig() {
        return multiSig;
    }

    public void setMultiSig(String multiSig) {
        this.multiSig = multiSig;
    }
}