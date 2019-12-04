package com.gd.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

public class IpfixKey {

    @QuerySqlField(name = "ip")
    @AffinityKeyMapped
    private String ip;

    @QuerySqlField(name = "url")
    private String url;

    @QuerySqlField(name = "event_time")
    private Date eventTime;

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

    public IpfixKey() {

    }

    public IpfixKey(@NotNull Ipfix ipfix) {
        this.ip = ipfix.getIp();
        this.url = ipfix.getUrl();
        this.eventTime = ipfix.getEventTime();

    }

    @Override
    public String toString() {
        return "IpfixKey{" +
                "ip='" + ip + '\'' +
                ", url='" + url + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
