package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureKeepAliveUpdate extends TableServiceEntity {
    private static final String MONITORING = "Monitoring";
    private static final String MATCHING_ENGINE = "MatchingEngine";

    private Date dateTime;

    public AzureKeepAliveUpdate() {
        super(MONITORING, MATCHING_ENGINE);
    }

    public AzureKeepAliveUpdate(Date date) {
        super(MONITORING, MATCHING_ENGINE);
        this.dateTime = date;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }
}