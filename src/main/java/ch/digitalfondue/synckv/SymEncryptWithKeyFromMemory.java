package ch.digitalfondue.synckv;

import org.jgroups.protocols.Encrypt;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class SymEncryptWithKeyFromMemory extends Encrypt<KeyStore.SecretKeyEntry> {

    private final String password;

    public SymEncryptWithKeyFromMemory(String password) {
        this.password = password;
    }

    @Override
    public void init() throws Exception {

        this.sym_keylength = 256;
        this.sym_algorithm = "AES";

        if (this.secret_key == null) {
            prepareSecretKey();
        }
        super.init();
    }

    private void prepareSecretKey() {
        this.setKeyStoreEntry(null);
    }

    @Override
    public void setKeyStoreEntry(KeyStore.SecretKeyEntry entry) {
        SecretKeySpec secretKeySpec = new SecretKeySpec(stretchKey(password).getEncoded(), "AES");
        this.secret_key = secretKeySpec;
    }

    private static SecretKey stretchKey(String password) {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), new byte[]{0}, 4096, 256);
            return factory.generateSecret(spec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException();
        }
    }
}