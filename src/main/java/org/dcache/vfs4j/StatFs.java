package org.dcache.vfs4j;

import com.google.common.base.MoreObjects;
import jdk.incubator.foreign.MemoryLayout;
import jnr.ffi.Struct;

import java.nio.ByteOrder;

/** */
public class StatFs extends Struct {

  public StatFs(jnr.ffi.Runtime runtime) {
    super(runtime);

    MemoryLayout layout = MemoryLayout.ofStruct(
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("type"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("bsize"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("block"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("free"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("bavail"),

            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("files"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("ffree"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("fsid"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("namelen"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("frsize"),
            MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("flags"),
            MemoryLayout.ofSequence(6, MemoryLayout.ofValueBits(Integer.SIZE, ByteOrder.nativeOrder())).withName("spare")
            );
  }

  public final Unsigned64 f_type = new Unsigned64();
  public final Unsigned64 f_bsize = new Unsigned64();
  public final Unsigned64 f_blocks = new Unsigned64();
  public final Unsigned64 f_bfree = new Unsigned64();
  public final Unsigned64 f_bavail = new Unsigned64();

  public final Unsigned64 f_files = new Unsigned64();
  public final Unsigned64 f_ffree = new Unsigned64();
  public final Unsigned64 f_fsid = new Unsigned64();
  public final Unsigned64 f_namelen = new Unsigned64();
  public final Unsigned64 f_frsize = new Unsigned64();
  public final Unsigned64 f_flags = new Unsigned64();

  public final Unsigned32[] __f_spare = array(new Unsigned32[6]);

  @Override
  public java.lang.String toString() {
    return MoreObjects.toStringHelper("statfs")
        .add("f_type", f_type.get())
        .add("f_bsize", f_bsize.get())
        .add("f_blocks", f_blocks.get())
        .add("f_bfree", f_bfree.get())
        .add("f_bavail", f_bavail.get())
        .add("f_files", f_files.get())
        .add("f_ffree", f_ffree.get())
        .add("f_fsid", f_fsid.get())
        .add("f_namelen", f_namelen.get())
        .add("f_frsize", f_frsize.get())
        .add("f_flags", f_flags.get())
        .toString();
  }
}
