package org.dcache.vfs4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TLSUtils {

  private TLSUtils() {}

  public static SSLContext createSslContext(
      String certificateFile,
      String certificateKeyFile,
      char[] keyPassword,
      String trustStore,
      boolean insecure)
      throws IOException, GeneralSecurityException {

    X509ExtendedKeyManager keyManager =
        PemUtils.loadIdentityMaterial(Paths.get(certificateFile), Paths.get(certificateKeyFile));
    X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(Paths.get(trustStore));

    var sslFactoryBuilder = SSLFactory.builder().withIdentityMaterial(keyManager);

    if (insecure) {
      sslFactoryBuilder = sslFactoryBuilder.withDummyTrustMaterial();
    } else {
      sslFactoryBuilder = sslFactoryBuilder.withTrustMaterial(trustManager);
    }

    return sslFactoryBuilder.build().getSslContext();
  }
}
