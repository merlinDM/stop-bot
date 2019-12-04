package com.gd.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.util.Date;
import java.util.Objects;

public class Ipfix {

    @QuerySqlField(name = "ip")
    private String ip;

    @QuerySqlField(name = "url")
    private String url;

    @QuerySqlField(name = "event_time")
    private Date eventTime;

    @QuerySqlField(name = "event_type")
    private String eventType;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Ipfix() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ipfix ipfix = (Ipfix) o;
        return ip.equals(ipfix.ip) &&
                url.equals(ipfix.url) &&
                eventTime.equals(ipfix.eventTime) &&
                Objects.equals(eventType, ipfix.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, url, eventTime, eventType);
    }

    @Override
    public String toString() {
        return "Ipfix{" +
                "ip='" + ip + '\'' +
                ", url='" + url + '\'' +
                ", eventTime=" + eventTime +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
