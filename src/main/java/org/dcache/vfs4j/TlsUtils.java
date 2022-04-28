package org.dcache.vfs4j;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.DirectoryCertChainValidator;
import eu.emi.security.authn.x509.impl.PEMCredential;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

public class TlsUtils {

    private TlsUtils() {}

    public static SSLContext createSslContext(
            String certificateFile, String certificateKeyFile, char[] keyPassword, String trustStore)
            throws IOException, GeneralSecurityException {

        X509CertChainValidatorExt certificateValidator =
                new DirectoryCertChainValidator(
                        List.of(trustStore), CertificateUtils.Encoding.PEM, -1, 5000, null);

        PEMCredential serviceCredentials =
                new PEMCredential(certificateKeyFile, certificateFile, keyPassword);

        KeyManager keyManager = serviceCredentials.getKeyManager();
        KeyManager[] kms = new KeyManager[] {keyManager};
        SSLTrustManager tm = new SSLTrustManager(certificateValidator);

        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(kms, new TrustManager[] {tm}, null);

        return sslCtx;
    }
}
