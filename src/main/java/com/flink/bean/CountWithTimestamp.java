package com.flink.bean;

public class CountWithTimestamp {
    private String key;
    private Long count;
    private Long lastModified;

    public CountWithTimestamp() {
        this.key = "";
        this.count = 0L;
        this.lastModified = 0L;
    }

    public CountWithTimestamp(String key, Long count, Long lastModified) {
        this.key = key;
        this.count = count;
        this.lastModified = lastModified;
    }

    public String getKey() {
        return key;
    }

    public Long getCount() {
        return count;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }
}
