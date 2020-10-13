package com.intel.oap.common.storage.pmemblk;

public interface KVDatabase {
    void put(String key, String value);
    void remove(String key);
    String getCopy(String key);
}
