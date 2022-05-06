package ch.digitalfondue.synckv;

import org.jgroups.protocols.Encrypt;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

class SymEncryptWithKeyFromMemory extends Encrypt<KeyStore.SecretKeyEntry> {

    private final String password;

    public SymEncryptWithKeyFromMemory(String password) {
        this.password = password;
    }

    @Override
    public <T extends Encrypt<KeyStore.SecretKeyEntry>> T setKeyStoreEntry(KeyStore.SecretKeyEntry secretKeyEntry) {
        this.secret_key = new SecretKeySpec(stretchKey(password).getEncoded(), "AES");
        return (T) this;
    }

    @Override
    public void init() throws Exception {

        this.sym_keylength = 256;
        this.sym_algorithm = "AES";

        if (this.secret_key == null) {
            this.setKeyStoreEntry(null);
        }
        super.init();
    }

    private static SecretKey stretchKey(String password) {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            return factory.generateSecret(new PBEKeySpec(password.toCharArray(), new byte[]{0}, 4096, 256));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException();
        }
    }
}
