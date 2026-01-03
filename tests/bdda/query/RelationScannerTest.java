package bdda.query;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import bdda.storage.Relation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RelationScannerTest {

    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private Relation relation;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Configuration de base
        config = DBConfig.LoadDBConfig(new File("config/config.txt"));
        diskManager = new DiskManager(config);
        bufferManager = new BufferManager(config, diskManager);

        // Création d'une relation "Etudiants" : [Age: INT, Moyenne: FLOAT, Nom: CHAR(10)]
        List<ColumnInfo> columns = Arrays.asList(
            new ColumnInfo("Age", "INT"),
            new ColumnInfo("Moyenne", "FLOAT"),
            new ColumnInfo("Nom", "CHAR(10)")
        );
        relation = new Relation("Etudiants", columns, diskManager, bufferManager);

        // Insertion de données de test (4 records)
        // R1: 20, 15.5, Alice
        // R2: 22, 10.0, Bob
        // R3: 20, 18.0, Charlie
        // R4: 25, 08.5, David
        relation.InsertRecord(new Record(Arrays.asList(20, 15.5f, "Alice")));
        relation.InsertRecord(new Record(Arrays.asList(22, 10.0f, "Bob")));
        relation.InsertRecord(new Record(Arrays.asList(20, 18.0f, "Charlie")));
        relation.InsertRecord(new Record(Arrays.asList(25, 08.5f, "David")));
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.finish();
    }

    // --- TESTS POUR RELATION SCANNER (SANS CONDITION) ---

    @Test
    void testRelationScannerFullScan() throws IOException {
        RelationScanner scanner = new RelationScanner(relation);
        List<Record> results = new ArrayList<>();
        
        Record r;
        while ((r = scanner.GetNextRecord()) != null) {
            results.add(r);
        }
        
        // On doit avoir lu les 4 records
        assertEquals(4, results.size());
        
        // Vérifions le contenu du premier (ordre d'insertion)
        assertEquals("Alice", results.get(0).getValue(2));
        assertEquals(20, results.get(0).getValue(0));
        
        scanner.Close();
    }

    @Test
    void testRelationScannerReset() throws IOException {
        RelationScanner scanner = new RelationScanner(relation);
        
        // Lire 2 records
        assertNotNull(scanner.GetNextRecord());
        assertNotNull(scanner.GetNextRecord());
        
        // Reset -> repartir du début
        scanner.Reset();
        
        // Lire tous les records (doit en trouver 4)
        int count = 0;
        while (scanner.GetNextRecord() != null) {
            count++;
        }
        assertEquals(4, count);
        
        scanner.Close();
    }

    // --- TESTS POUR RELATION SCANNER WITH SELECT (AVEC CONDITIONS) ---

    @Test
    void testScannerSelectSimpleCondition() throws IOException {
        // Condition : Age = 20
        Condition cond = new Condition(0, null, Condition.OP_EQUAL, -1, 20);
        List<Condition> conditions = Collections.singletonList(cond);

        RelationScannerWithSelect scanner = new RelationScannerWithSelect(relation, conditions);
        
        List<Record> results = new ArrayList<>();
        Record r;
        while ((r = scanner.GetNextRecord()) != null) {
            results.add(r);
        }

        // Doit trouver Alice (20) et Charlie (20) -> 2 records
        assertEquals(2, results.size());
        
        // Vérifions les noms
        List<String> names = new ArrayList<>();
        for(Record rec : results) names.add((String)rec.getValue(2));
        
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Charlie"));
        assertFalse(names.contains("Bob")); // Bob a 22 ans
    }

    @Test
    void testScannerSelectMultipleConditions() throws IOException {
        // Condition 1 : Age >= 20
        Condition c1 = new Condition(0, null, Condition.OP_GREATER_EQUAL, -1, 20);
        // Condition 2 : Moyenne > 12.0
        Condition c2 = new Condition(1, null, Condition.OP_GREATER, -1, 12.0f);
        
        List<Condition> conditions = Arrays.asList(c1, c2);

        RelationScannerWithSelect scanner = new RelationScannerWithSelect(relation, conditions);
        
        List<Record> results = new ArrayList<>();
        Record r;
        while ((r = scanner.GetNextRecord()) != null) {
            results.add(r);
        }

        // Alice (20, 15.5) -> OK
        // Bob (22, 10.0) -> KO (Moyenne)
        // Charlie (20, 18.0) -> OK
        // David (25, 08.5) -> KO (Moyenne)
        
        assertEquals(2, results.size());
    }

    @Test
    void testScannerSelectNoMatch() throws IOException {
        // Condition : Age > 100
        Condition cond = new Condition(0, null, Condition.OP_GREATER, -1, 100);
        List<Condition> conditions = Collections.singletonList(cond);

        RelationScannerWithSelect scanner = new RelationScannerWithSelect(relation, conditions);
        
        // Aucun record ne doit être retourné
        assertNull(scanner.GetNextRecord());
    }

    @Test
    void testScannerSelectReset() throws IOException {
        // Condition : Age = 22 (Bob)
        Condition cond = new Condition(0, null, Condition.OP_EQUAL, -1, 22);
        RelationScannerWithSelect scanner = new RelationScannerWithSelect(relation, Collections.singletonList(cond));

        // Lire une fois
        Record r1 = scanner.GetNextRecord();
        assertNotNull(r1);
        assertEquals("Bob", r1.getValue(2));
        assertNull(scanner.GetNextRecord()); // Fin

        // Reset
        scanner.Reset();
        
        // Relire
        Record r2 = scanner.GetNextRecord();
        assertNotNull(r2);
        assertEquals("Bob", r2.getValue(2));
    }
}