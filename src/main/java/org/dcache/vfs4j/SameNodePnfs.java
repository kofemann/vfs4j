package org.dcache.vfs4j;

import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.FlexFileLayoutDriver;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.LayoutDriver;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.Stateids;
import org.dcache.nfs.v4.ff.flex_files_prot;
import org.dcache.nfs.v4.xdr.GETDEVICEINFO4args;
import org.dcache.nfs.v4.xdr.GETDEVICELIST4args;
import org.dcache.nfs.v4.xdr.LAYOUTCOMMIT4args;
import org.dcache.nfs.v4.xdr.LAYOUTERROR4args;
import org.dcache.nfs.v4.xdr.LAYOUTGET4args;
import org.dcache.nfs.v4.xdr.LAYOUTRETURN4args;
import org.dcache.nfs.v4.xdr.LAYOUTSTATS4args;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layout4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.length4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.offset4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.utf8str_mixed;
import org.dcache.nfs.vfs.Inode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SameNodePnfs implements NFSv41DeviceManager {

  public record OpenFile(clientid4 clientid, Inode inode) {}

  private final Map<OpenFile, NFS4State> _openToLayoutStateid = new ConcurrentHashMap<>();

  private final LayoutDriver layoutDriver =
      new FlexFileLayoutDriver(
          4,
          1,
          flex_files_prot.FF_FLAGS_NO_IO_THRU_MDS,
          1048576, // 1MB
          new utf8str_mixed("17"),
          new utf8str_mixed("17"),
          (c, s) -> {});

  private static final deviceid4 deviceid;

  static {
    deviceid = new deviceid4(new byte[nfs4_prot.NFS4_DEVICEID4_SIZE]);
    deviceid.value[0] = 0x17;
  }

  private final InetSocketAddress[] localAddresses;

  /**
   */
  private device_addr4 deviceAddress;


  private final LocalVFS vfs;

  public SameNodePnfs(LocalVFS vfs, int port) throws IOException {
    this.vfs = vfs;
    localAddresses = getLocalAddresses(port);
    deviceAddress = layoutDriver.getDeviceAddress(localAddresses);

    startDS(port);
  }


  private void startDS(int port) throws IOException {

  }


  @Override
  public Layout layoutGet(CompoundContext context, LAYOUTGET4args args) throws IOException {

    final NFS4Client client = context.getSession().getClient();
    final stateid4 stateid = Stateids.getCurrentStateidIfNeeded(context, args.loga_stateid);
    final NFS4State nfsState = client.state(stateid);

    Inode inode = context.currentInode();

    NFS4State openState = nfsState.getOpenState();
    var ioKey = new OpenFile(client.getId(), inode);

    NFS4State layoutStateId = _openToLayoutStateid.get(ioKey);
    if (layoutStateId == null) {
      layoutStateId = client.createLayoutState(openState.getStateOwner());
      _openToLayoutStateid.put(ioKey, layoutStateId);
    }
    layoutStateId.bumpSeqid();

    nfs_fh4 fh = new nfs_fh4(inode.toNfsHandle());

    //  -1 is special value, which means entire file
    layout4 layout = new layout4();
    layout.lo_iomode = args.loga_iomode;
    layout.lo_offset = new offset4(0);
    layout.lo_length = new length4(nfs4_prot.NFS4_UINT64_MAX);
    layout.lo_content =
        layoutDriver.getLayoutContent(layoutStateId.stateid(), NFSv4Defaults.NFS4_STRIPE_SIZE, fh, deviceid);

    return new Layout(true, layoutStateId.stateid(), new layout4[] {layout});
  }

  @Override
  public device_addr4 getDeviceInfo(
      CompoundContext compoundContext, GETDEVICEINFO4args getdeviceinfo4args) throws IOException {

    return deviceAddress;
  }

  @Override
  public List<deviceid4> getDeviceList(
      CompoundContext compoundContext, GETDEVICELIST4args getdevicelist4args) throws IOException {
    return List.of();
  }

  @Override
  public void layoutReturn(CompoundContext compoundContext, LAYOUTRETURN4args layoutreturn4args)
      throws IOException {}

  @Override
  public OptionalLong layoutCommit(
      CompoundContext compoundContext, LAYOUTCOMMIT4args layoutcommit4args) throws IOException {
    return OptionalLong.empty();
  }

  @Override
  public void layoutStats(CompoundContext compoundContext, LAYOUTSTATS4args layoutstats4args)
      throws IOException {}

  @Override
  public void layoutError(CompoundContext compoundContext, LAYOUTERROR4args layouterror4args)
      throws IOException {}

  @Override
  public Set<layouttype4> getLayoutTypes() {
    return Set.of(layoutDriver.getLayoutType());
  }

  private static boolean isUp(NetworkInterface iface) {
    try {
      return iface.isUp() && !iface.isLoopback() && !iface.getName().startsWith("br-");
    } catch (SocketException e) {
      return false;
    }
  }

  public static InetSocketAddress[] getLocalAddresses(int port) throws SocketException {

    return NetworkInterface.networkInterfaces()
        .filter(SameNodePnfs::isUp)
        .flatMap(NetworkInterface::inetAddresses)
        .map(a -> new InetSocketAddress(a, port))
        .toArray(InetSocketAddress[]::new);
  }
}
