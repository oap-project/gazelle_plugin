package com.intel.oap.common.storage.pmemblk;

public class PMemKVDatabase {

    private static KVDatabase db;

    public static KVDatabase open(String engine, String path, long size) {
        try {
            db =  (KVDatabase) Class.forName("io.pmem.pmemkv.Database").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return db;
    }

    public static void close() {
        // ToDo: do nothing here for the time being
    }

}