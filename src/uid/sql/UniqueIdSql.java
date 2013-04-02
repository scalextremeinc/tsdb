package net.opentsdb.uid.sql;

import net.opentsdb.uid.UniqueIdInterface;

public final class UniqueIdSql implements UniqueIdInterface {
    
    String kind() {
        return null;
    }

    short width() {
        return 0;
    }

    String getName(byte[] id) throws NoSuchUniqueId {
        return null;
    }

    byte[] getId(String name) throws NoSuchUniqueName {
        return null;
    }

    byte[] getOrCreateId(String name) {
        return null;
    }
    
}
