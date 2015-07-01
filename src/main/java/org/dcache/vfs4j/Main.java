package org.dcache.vfs4j;

import java.io.File;
import java.io.IOException;

public class Main {

    // sudo java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -jar target/vfs4j-1.0-SNAPSHOT-jar-with-dependencies.jar
    public static void main(String args[]) throws IOException {

        LocalVFS vfs = new LocalVFS(new File("/tmp"));
        System.out.println(vfs.lookup(vfs.getRootInode(), "aa"));
        System.out.println(vfs.getRootInode());

    }
}
