package bdda.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DiskManagerTest {

    private DiskManager diskManager;
    private DBConfig config;
    private final int PAGE_SIZE = 4096;
    private final int BITMAP_SIZE = 8192;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Configuration initiale avec dossier temporaire
        config = new DBConfig(tempDir.toString(), PAGE_SIZE, 2, 10, BufferPolicy.LRU);
        diskManager = new DiskManager(config);
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.finish();
    }

    @Test
    void testAllocPage() throws IOException {
        // Allocation d'une nouvelle page
        PageId pageId = diskManager.allocPage();

        assertNotNull(pageId);
        assertEquals(0, pageId.getFileIdx());
        assertEquals(0, pageId.getPageIdx());

        // Vérification de la création du fichier (Bitmap + 1 page)
        File f = tempDir.resolve("Data0.bin").toFile();
        assertTrue(f.exists());
        assertEquals(BITMAP_SIZE + PAGE_SIZE, f.length());
    }

    @Test
    void testWriteAndReadPage() throws IOException {
        PageId pageId = diskManager.allocPage();
        byte[] dataToWrite = new byte[PAGE_SIZE];
        new Random().nextBytes(dataToWrite);

        // Écriture
        diskManager.WritePage(pageId, dataToWrite);

        // Lecture
        byte[] dataRead = new byte[PAGE_SIZE];
        diskManager.ReadPage(pageId, dataRead);

        // Vérification du contenu
        assertArrayEquals(dataToWrite, dataRead);
    }

    @Test
    void testDeallocAndReuse() throws IOException {
        PageId p1 = diskManager.allocPage(); // Page 0
        diskManager.allocPage();             // Page 1

        // Désallocation de la page 0
        diskManager.DeallocPage(p1);

        // La prochaine allocation doit réutiliser la page 0
        PageId p3 = diskManager.allocPage();
        
        assertEquals(p1.getPageIdx(), p3.getPageIdx(), "La page libérée doit être réutilisée");
    }

    @Test
    void testPersistence() throws IOException {
        // Allocation et sauvegarde
        diskManager.allocPage(); // Index 0
        diskManager.finish();

        // Rechargement d'un nouveau DiskManager
        DiskManager dm2 = new DiskManager(config);
        
        // La nouvelle page ne doit PAS écraser l'index 0 (car persisté comme utilisé)
        PageId newPage = dm2.allocPage();
        
        assertEquals(1, newPage.getPageIdx());
    }

    @Test
    void testReadInvalidPageSize() throws IOException {
        PageId p1 = diskManager.allocPage();
        byte[] bufferIncorrect = new byte[10];

        // Doit échouer si le buffer n'a pas la bonne taille
        assertThrows(IOException.class, () -> diskManager.ReadPage(p1, bufferIncorrect));
    }

    @Test
    void testAccessNonExistentPage() {
        PageId invalidId = new PageId(0, 999);
        byte[] buffer = new byte[PAGE_SIZE];

        // Doit échouer si la page n'existe pas physiquement
        assertThrows(IOException.class, () -> diskManager.ReadPage(invalidId, buffer));
    }

    @Test
    void testMultiFileAllocation() throws IOException {
        // Remplir artificiellement le premier fichier (simulation)
        // Note: Ce test dépend de MAX_PAGES_PER_FILE dans DiskManager
        // Ici, on vérifie simplement que l'allocation continue fonctionne
        for(int i=0; i<10; i++) {
            assertNotNull(diskManager.allocPage());
        }
    }
}