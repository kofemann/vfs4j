/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.vfs4j;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author tigran
 */
public class Main {

    public static void main(String args[]) throws IOException {

        LocalVFS vfs = new LocalVFS(new File("/tmp"));
        System.out.println(vfs.getRootInode());

    }
}
