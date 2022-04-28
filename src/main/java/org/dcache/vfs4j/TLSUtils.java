package org.dcache.vfs4j;

import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.PEMCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

public class TLSUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TLSUtils.class);

  private TLSUtils() {}

  public static SSLContext createSslContext(
      String certificateFile, String certificateKeyFile, char[] keyPassword, String trustStore, boolean insecure)
      throws IOException, GeneralSecurityException {

    TrustManager tm;
    if (insecure) {
      tm = new HappyX509TrustManager();
    } else {
      tm = new FileBasedX509TrustManager(trustStore);
    }

    PEMCredential serviceCredentials =
        new PEMCredential(certificateKeyFile, certificateFile, keyPassword);

    KeyManager keyManager = serviceCredentials.getKeyManager();
    KeyManager[] kms = new KeyManager[] {keyManager};

    SSLContext sslCtx = SSLContext.getInstance("TLS");
    sslCtx.init(kms, new TrustManager[] {tm}, null);

    return sslCtx;
  }

  private static class HappyX509TrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private static class FileBasedX509TrustManager implements X509TrustManager {
    private final X509Certificate[] trusted;

    public FileBasedX509TrustManager(String store) throws IOException {
      trusted =
          CertificateUtils.loadCertificateChain(
              Files.newInputStream(Path.of(store)), CertificateUtils.Encoding.PEM);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      validate(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      validate(chain, authType);
    }

    private void validate(X509Certificate[] chain, String authType) throws CertificateException {

      for (X509Certificate c : chain) {
        try {
          c.checkValidity();
        } catch (CertificateNotYetValidException e) {
          LOGGER.warn(
              "Certificate for {} is not valid yet - {}",
              c.getSubjectX500Principal().getName(),
              e.getMessage());
          throw e;
        } catch (CertificateExpiredException e) {
          LOGGER.warn(
              "Certificate for {} is expired - {}",
              c.getSubjectX500Principal().getName(),
              e.getMessage());
          throw e;
        }

        boolean signed = false;
        signer:
        for (X509Certificate signer : trusted) {
          try {
            c.verify(signer.getPublicKey());
            signed = true;
            break signer;
          } catch (NoSuchAlgorithmException
              | InvalidKeyException
              | NoSuchProviderException
              | SignatureException e) {
            LOGGER.warn("Failed to verify signature for {} : {}", c.getSubjectX500Principal().getName(), e.getMessage());
          }
        }
        if (!signed) {
          LOGGER.warn("No trusted anchor found for {}", c.getSubjectX500Principal().getName());
          throw new CertificateException("No trusted anchor found for: " + c.getSubjectX500Principal().getName());
        }
      }

      LOGGER.info("Certificate chain validated for {}", chain[0].getSubjectX500Principal().getName());
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return trusted;
    }
  }
}
