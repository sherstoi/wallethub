package com.ef.model;

import java.time.LocalDateTime;

public class ActiveIP {
    String ip;
    LocalDateTime startDate;
    String durationType;
    Integer reqCount;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDateTime startDate) {
        this.startDate = startDate;
    }

    public String getDurationType() {
        return durationType;
    }

    public void setDurationType(String durationType) {
        this.durationType = durationType;
    }

    public Integer getReqCount() {
        return reqCount;
    }

    public void setReqCount(Integer reqCount) {
        this.reqCount = reqCount;
    }

    @Override
    public String toString() {
        return "ActiveIP{" +
                "ip='" + ip + '\'' +
                ", startDate=" + startDate +
                ", durationType='" + durationType + '\'' +
                ", reqCount=" + reqCount +
                '}';
    }
}
