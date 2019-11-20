package io.joshworks.lsm.server;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Admin {

    private final Map<String, LsmTree<String, byte[]>> namespaces = new ConcurrentHashMap<>();
    private final File root;

    public Admin(File root) {
        this.root = root;
        if (root.exists()) {
            File[] files = root.listFiles();
            if (files == null) {
                return;
            }
            for (File f : files) {
                if (f.isDirectory()) {
                    openStore(f);
                }
            }
        }
    }

    public LsmTree<String, byte[]> createNamespace(String name) {
        if (namespaces.containsKey(name)) {
            throw new RuntimeException("Namespace with name '" + name + "' already exist");
        }
        File namespaceRoot = new File(root, name);
        LsmTree<String, byte[]> store = openStore(namespaceRoot);
        namespaces.put(name, store);
        return store;
    }

    public LsmTree<String, byte[]> namespace(String name) {
        return namespaces.get(name);
    }


    private LsmTree<String, byte[]> openStore(File rootDir) {
        return LsmTree.builder(rootDir, Serializers.VSTRING, Serializers.VLEN_BYTE_ARRAY)
                .sstableStorageMode(StorageMode.MMAP)
                .name(rootDir.getName())
                .transactionLogStorageMode(StorageMode.MMAP)
                .open();
    }

}
