package org.dcache.vfs4j;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.ExportTable;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.MDSOperationExecutor;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;
import org.slf4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

@CommandLine.Command(name = "nfs4j", mixinStandardHelpOptions = true, version = "0.0.1", showDefaultValues = true)
public class NfsMain implements Callable<Void> {

  static {
    // Redirect java.util.logging to SLF4J
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();
  }

  private final static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(NfsMain.class);

  @CommandLine.Option(
      names = {"--cert"},
      description = "PEM encoded host certificate",
      defaultValue = "hostcert.pem")
  private String cert;

  @CommandLine.Option(
      names = {"--key"},
      description = "PEM encoded host key",
      defaultValue = "hostkey.pem")
  private String key;

  @CommandLine.Option(
      names = "--ca-chain",
      description = "Trusted CA chain",
      defaultValue = "ca-chain.pem")
  private String chain;

  @CommandLine.Option(
      names = {"--with-tls"},
      description = "Enable RPC-over-TLS",
      defaultValue = "false")
  private boolean tls;

  @CommandLine.Option(
          names = {"--with-mutual-tls"},
          description = "Enable mutual TLS authentication",
          defaultValue = "false")
  private boolean mutual;

    @CommandLine.Option(
            names = {"--insecure"},
            description = "Skip TLS certificate chain verification step",
            defaultValue = "false")
    private boolean insecure;

  @CommandLine.Option(
          names = {"--with-v3"},
          negatable = true,
          description = "Enable NFS version 3",
          showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
          defaultValue = "false")
  private boolean withV3;

  @CommandLine.Option(
          names = {"--with-v4"},
          negatable = true,
          description = "Enable NFS version 4.1",
          showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
          defaultValue = "true")
  private boolean withV4;


  @CommandLine.Option(
          names = {"--pnfs"},
          negatable = true,
          description = "Enable pNFS",
          showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
          defaultValue = "true")
  private boolean withPnfs;


  @CommandLine.Option(
          names = {"--port"},
          negatable = false,
          description = "Specify NFS server port to listen on",
          showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
          defaultValue = "2049")
  private int port;

  @CommandLine.Option(
          names = {"--ds-port"},
          negatable = false,
          description = "Specify pNFS DS port to listen on",
          showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
          defaultValue = "2053")
  private int dsPort;

  @CommandLine.Parameters(index = "0", description = "directory to export")
  private File dir;

  @CommandLine.Parameters(index = "1", description = "path to export file")
  private File export;

  public static void main(String[] args) throws Exception {
    new CommandLine(new NfsMain()).setNegatableOptionTransformer(
            new CommandLine.RegexTransformer.Builder()
                    .addPattern("with", "without", "with[out]")
                    .addPattern("^--with((?i)out)-(\\w(-|\\w)*)$", "--with-$2", "--with[$1-]$2")
                    .build()
    ).execute(args);
  }

  @Override
  public Void call() throws Exception {

    SSLParameters sslParameters = null;
    SSLContext sslContext = null;
    if (tls) {
      sslContext = TLSUtils.createSslContext(cert, key, new char[0], chain, insecure);
      sslParameters = sslContext.getDefaultSSLParameters();
      sslParameters.setNeedClientAuth(mutual);
    }


    LocalVFS vfs = new LocalVFS(dir);

    NFSv41DeviceManager pnfs = null;

    if (withPnfs) {
      OncRpcSvc nfsSvc =
          new OncRpcSvcBuilder()
              .withPort(dsPort)
              .withTCP()
              .withoutAutoPublish()
              .withWorkerThreadIoStrategy()
              .withWorkerThreadExecutionService(Executors.newVirtualThreadPerTaskExecutor())
              .withStartTLS()
              .withSSLContext(sslContext)
              .withSSLParameters(sslParameters)
              .withServiceName("pNFS/DS@" + dsPort)
              .build();

      NFSServerV41 nfs4 =
          new NFSServerV41.Builder()
              .withVfs(vfs)
              .withOperationExecutor(new EDSNFSv4OperationFactory(vfs))
              .build();
      nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);

      nfsSvc.start();
      pnfs = new SameNodePnfs(vfs, dsPort);
    }

    OncRpcSvc nfsSvc =
        new OncRpcSvcBuilder()
            .withPort(port)
            .withTCP()
            .withAutoPublish()
            .withWorkerThreadIoStrategy()
            .withWorkerThreadExecutionService(Executors.newVirtualThreadPerTaskExecutor())
            .withStartTLS()
            .withSSLContext(sslContext)
            .withSSLParameters(sslParameters)
            .withServiceName("nfsd@" + port)
            .build();

    ExportTable exportFile = new ExportFile(export);

    if (withV4) {
      NFSServerV41 nfs4 =
        new NFSServerV41.Builder()
            .withExportTable(exportFile)
            .withVfs(vfs)
            .withOperationExecutor(new MDSOperationExecutor())
            .withDeviceManager(pnfs)
            .build();
      nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
    }

    if (withV3) {
      NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
      MountServer mountd = new MountServer(exportFile, vfs);
      nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
      nfsSvc.register(new OncRpcProgram(100005, 3), mountd);
    }

    nfsSvc.start();
    LOGGER.info("Starting NFS service on port {}", nfsSvc.getInetSocketAddress(IpProtocolType.TCP).getPort());

    Thread.currentThread().join();
    return null;
  }
}
