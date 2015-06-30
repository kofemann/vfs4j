package org.dcache.vfs4j;

import jnr.ffi.Struct;
/**
 *
 * @author tigran
 */
public class KernelFileHandle extends Struct {

    protected KernelFileHandle(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public final u_int32_t handleBytes = new u_int32_t();

    public final int32_t handleType = new int32_t();

    public final Signed8[] handleData = array(new Signed8[1024]);

    private final static char[] HEX = new char[]{
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    /**
     * Returns a hexadecimal representation of given byte array.
     *
     * @param bytes whose string representation to return
     * @return a string representation of <tt>bytes</tt>
     */
    public static java.lang.String toHexString(Signed8[] bytes, int len) {

        char[] chars = new char[bytes.length * 2];
        int p = 0;
        for (int i = 0; i < len; i++) {
            int b = bytes[i].get() & 0xff;
            chars[p++] = HEX[b / 16];
            chars[p++] = HEX[b % 16];
        }
        return new java.lang.String(chars);
    }

    public java.lang.String toString() {
        return "[" + toHexString(handleData, handleBytes.intValue()) + "],"
                + " len = " + handleBytes.intValue() + ", type = " + handleType.intValue();
    }
}
