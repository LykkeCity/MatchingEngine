package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.util.Date;

public class KeepAliveUpdate extends TableServiceEntity {
    public static String MONITORING = "Monitoring";
    public static String MATCHING_ENGINE = "MatchingEngine";

    Date dateTime;

    public KeepAliveUpdate() {
        super(MONITORING, MATCHING_ENGINE);
    }

    public KeepAliveUpdate(Date date) {
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