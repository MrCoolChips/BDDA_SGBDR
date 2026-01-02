package bdda.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BufferManagerTest {

    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;

    @BeforeEach
    public void setUp() throws IOException {
        File configFile = new File("config/config.txt");
        if (!configFile.exists()) {
            throw new IOException("Fichier de configuration introuvable.");
        }
        this.config = DBConfig.LoadDBConfig(configFile);
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
    }

    @AfterEach
    public void tearDown() throws IOException {
        bufferManager.FlushBuffers();
        diskManager.finish();
    }

    @Test
    public void testGestionPinCount() throws IOException {
        PageId pid = diskManager.allocPage();

        // 1. Premier acces -> PinCount doit etre 1
        bufferManager.GetPage(pid);
        Frame frame = getFrameFromBuffer(pid);
        
        assertNotNull(frame, "La frame ne doit pas être null");
        assertEquals(1, frame.pinCount, "PinCount doit être à 1 après le premier accès");

        // 2. Deuxième acces -> PinCount doit etre 2
        bufferManager.GetPage(pid);
        assertEquals(2, frame.pinCount, "PinCount doit être à 2 après le second accès");

        // 3. Liberation -> PinCount doit etre 1
        bufferManager.FreePage(pid, false);
        assertEquals(1, frame.pinCount, "PinCount doit être à 1 après un FreePage");

        // 4. Liberation finale -> PinCount doit etre 0
        bufferManager.FreePage(pid, false);
        assertEquals(0, frame.pinCount, "PinCount doit être à 0 après libération totale");
    }

    @Test
    public void testDirtyFlagEtPersistence() throws IOException {
        PageId pid = diskManager.allocPage();
        String data = "Test Persistance JUnit";
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

        // Modification de la page
        byte[] buffer = bufferManager.GetPage(pid);
        System.arraycopy(dataBytes, 0, buffer, 0, dataBytes.length);

        // Liberation avec dirty = true
        bufferManager.FreePage(pid, true);

        Frame frame = getFrameFromBuffer(pid);
        assertTrue(frame.dirty, "Le flag Dirty doit être à true");

        // Force l'ecriture sur disque
        bufferManager.FlushBuffers();
        assertTrue(bufferManager.getPageTable().isEmpty(), "La PageTable doit être vide après Flush");

        // Relecture depuis le disque
        byte[] newBuffer = bufferManager.GetPage(pid);
        String readData = new String(newBuffer, 0, dataBytes.length, StandardCharsets.UTF_8);

        assertEquals(data, readData, "Les données lues sur le disque doivent correspondre aux modifications");
    }

    @Test
    public void testFlushBuffers() throws IOException {
    	
    	if (config.getBufferCount() < 2) {
            System.out.println("Les tests testFlushBuffers ont été ignorés car le nombre de tampons était inférieur à 2 (Il faut modifier config.txt)");
            return;
        }
    	
    	
        PageId p1 = diskManager.allocPage();
        PageId p2 = diskManager.allocPage();
        
        bufferManager.GetPage(p1);
        bufferManager.GetPage(p2);

        bufferManager.FreePage(p1, false);
        bufferManager.FreePage(p2, true);

        assertFalse(bufferManager.getPageTable().isEmpty());

        bufferManager.FlushBuffers();

        assertTrue(bufferManager.getPageTable().isEmpty(), "Tous les buffers doivent être libérés après Flush");
    }

    @Test
    public void testSaturationBuffer() throws IOException {
        int bufferCount = config.getBufferCount();
        
        // Remplir le buffer pool et garder les pages épinglées
        for (int i = 0; i < bufferCount; i++) {
            PageId pid = diskManager.allocPage();
            bufferManager.GetPage(pid);
        }

        // Tenter de charger une page supplementaire
        PageId extraPage = diskManager.allocPage();

        Exception exception = assertThrows(IOException.class, () -> {
            bufferManager.GetPage(extraPage);
        });

        String msg = exception.getMessage();
        assertTrue(msg.contains("toutes les frames sont épinglées") || msg.contains("saturé"), 
                   "Le message d'erreur doit indiquer la saturation");
    }

    // Methode utilitaire pour acceder a la Frame via la PageTable
    private Frame getFrameFromBuffer(PageId pid) {
        Map<String, Frame> pageTable = bufferManager.getPageTable();
        String key = pid.getFileIdx() + ":" + pid.getPageIdx();
        return pageTable.get(key);
    }
}