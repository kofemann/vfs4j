package org.dcache.vfs4j;

import jnr.ffi.Struct;

public class Dirent extends Struct {

  private static final int MAX_NAME_LEN = 255;

  public Dirent(jnr.ffi.Runtime runtime) {
    super(runtime);
  }

  public final Signed64 d_ino = new Signed64();
  public final Signed64 d_off = new Signed64();
  public final Unsigned16 d_reclen = new Unsigned16();
  public final Unsigned8 d_type = new Unsigned8();
  public final UTF8String d_name = new UTF8String(MAX_NAME_LEN);
}
