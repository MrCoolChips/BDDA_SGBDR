package bdda.core;

import bdda.manager.DBManager;
import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import bdda.storage.Relation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DBManagerTest {

    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBManager dbManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Initialisation de la configuration (chemin, pageSize, maxFile, buffCount, Policy)
        // Note: BufferPolicy.LRU est passé comme dernier paramètre
        config = new DBConfig(tempDir.toString(), 4096, 5, 10, BufferPolicy.LRU);
        
        diskManager = new DiskManager(config);
        bufferManager = new BufferManager(config, diskManager);
        dbManager = new DBManager(config, diskManager, bufferManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Nettoyage et fermeture
        dbManager.Finish();
    }

    @Test
    void testInitAndEmptyState() {
        // Vérifie l'état initial (0 tables)
        assertEquals(0, dbManager.GetTableCount());
        assertTrue(dbManager.GetTableNames().isEmpty());
    }

    @Test
    void testAddAndGetTable() throws IOException {
        // Création d'une table de test
        List<ColumnInfo> columns = Arrays.asList(new ColumnInfo("Col1", "INT"));
        Relation table = new Relation("TableTest", columns, diskManager, bufferManager);

        // Ajout à la DB
        dbManager.AddTable(table);

        // Vérification
        assertEquals(1, dbManager.GetTableCount());
        assertNotNull(dbManager.GetTable("TableTest"));
        assertEquals("TableTest", dbManager.GetTable("TableTest").getName());
    }

    @Test
    void testGetNonExistentTable() {
        // Doit retourner null si la table n'existe pas
        assertNull(dbManager.GetTable("Inconnu"));
    }

    @Test
    void testRemoveTable() throws IOException {
        // Ajout d'une table
        List<ColumnInfo> columns = Arrays.asList(new ColumnInfo("C1", "INT"));
        dbManager.AddTable(new Relation("T1", columns, diskManager, bufferManager));
        
        // Suppression
        dbManager.RemoveTable("T1");

        // Vérification
        assertEquals(0, dbManager.GetTableCount());
        assertNull(dbManager.GetTable("T1"));
    }

    @Test
    void testRemoveNonExistentTable() {
        // Ne doit pas planter si on supprime une table inexistante
        assertDoesNotThrow(() -> dbManager.RemoveTable("Fantome"));
    }

    @Test
    void testRemoveAllTables() throws IOException {
        // Ajout de plusieurs tables
        List<ColumnInfo> cols = Arrays.asList(new ColumnInfo("C", "INT"));
        dbManager.AddTable(new Relation("T1", cols, diskManager, bufferManager));
        dbManager.AddTable(new Relation("T2", cols, diskManager, bufferManager));

        // Suppression totale
        dbManager.RemoveAllTables();

        // Vérification
        assertEquals(0, dbManager.GetTableCount());
    }

    @Test
    void testSaveAndLoadState() throws IOException {
        // 1. Création et sauvegarde
        List<ColumnInfo> cols = Arrays.asList(new ColumnInfo("Id", "INT"), new ColumnInfo("Nom", "CHAR(10)"));
        Relation r = new Relation("Etudiants", cols, diskManager, bufferManager);
        dbManager.AddTable(r);
        
        dbManager.SaveState(); // Écrit dans database.save

        // 2. Rechargement dans une nouvelle instance
        DBManager dbManager2 = new DBManager(config, diskManager, bufferManager);
        dbManager2.LoadState();

        // Vérification de la persistance
        assertEquals(1, dbManager2.GetTableCount());
        Relation loaded = dbManager2.GetTable("Etudiants");
        assertNotNull(loaded);
        assertEquals(2, loaded.getColumnCount());
    }

    @Test
    void testPersistenceWithData() throws IOException {
        // 1. Création table et insertion données
        List<ColumnInfo> cols = Arrays.asList(new ColumnInfo("Val", "INT"));
        Relation table = new Relation("DataTest", cols, diskManager, bufferManager);
        dbManager.AddTable(table);

        table.InsertRecord(new Record(Arrays.asList(42)));
        table.InsertRecord(new Record(Arrays.asList(100)));

        // Sauvegarde et fermeture
        dbManager.Finish(); 

        // 2. Réouverture complète (Simulation redémarrage SGBD)
        DiskManager dm2 = new DiskManager(config);
        BufferManager bm2 = new BufferManager(config, dm2);
        DBManager dbManager2 = new DBManager(config, dm2, bm2);
        
        dbManager2.LoadState();

        // Vérification des données
        Relation t = dbManager2.GetTable("DataTest");
        assertNotNull(t);
        List<Record> records = t.GetAllRecords();
        
        assertEquals(2, records.size());
        assertEquals(42, records.get(0).getValues().get(0));
    }

    @Test
    void testDescribeTable() throws IOException {
        // Vérifie juste qu'aucune exception n'est levée
        List<ColumnInfo> cols = Arrays.asList(new ColumnInfo("A", "INT"));
        dbManager.AddTable(new Relation("T1", cols, diskManager, bufferManager));
        
        assertDoesNotThrow(() -> dbManager.DescribeTable("T1"));
        assertDoesNotThrow(() -> dbManager.DescribeAllTables());
    }
}