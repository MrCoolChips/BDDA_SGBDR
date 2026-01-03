package bdda.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DBConfigTest {

    @Test
    void testConstructorAndGetters() {
        // Initialisation
        String path = "/tmp/db";
        int pageSize = 4096;
        int maxFiles = 5;
        int bufferCount = 10;
        BufferPolicy policy = BufferPolicy.LRU;

        // Exécution
        DBConfig config = new DBConfig(path, pageSize, maxFiles, bufferCount, policy);

        // Vérification des valeurs
        assertEquals(path, config.getPath());
        assertEquals(pageSize, config.getPageSize());
        assertEquals(maxFiles, config.getMaxFileCount());
        assertEquals(bufferCount, config.getBufferCount());
        assertEquals(policy, config.getBufferPolicy());
    }

    @Test
    void testLoadDBConfigValid(@TempDir Path tempDir) throws IOException {
        // Préparation du fichier de configuration valide
        File configFile = tempDir.resolve("db.config").toFile();
        List<String> lines = List.of(
            "dbpath = '/usr/local/db'",
            "pagesize = 8192",
            "dm_maxfilecount = 20",
            "bm_buffercount = 100",
            "bm_policy = 'MRU'"
        );
        Files.write(configFile.toPath(), lines);

        // Chargement de la configuration
        DBConfig config = DBConfig.LoadDBConfig(configFile);

        assertNotNull(config);
        assertEquals("/usr/local/db", config.getPath());
        assertEquals(8192, config.getPageSize());
        assertEquals(BufferPolicy.MRU, config.getBufferPolicy());
    }

    @Test
    void testLoadDBConfigIncomplete(@TempDir Path tempDir) throws IOException {
        // Fichier incomplet (manque pagesize)
        File configFile = tempDir.resolve("incomplete.config").toFile();
        List<String> lines = List.of(
            "dbpath = '/usr/local/db'",
            "dm_maxfilecount = 20"
        );
        Files.write(configFile.toPath(), lines);

        DBConfig config = DBConfig.LoadDBConfig(configFile);

        // Doit retourner null car incomplet
        assertNull(config);
    }

    @Test
    void testLoadDBConfigFileNotFound() {
        File nonExistentFile = new File("imaginaire.config");

        // Vérification de l'exception IOException
        assertThrows(IOException.class, () -> {
            DBConfig.LoadDBConfig(nonExistentFile);
        });
    }
}