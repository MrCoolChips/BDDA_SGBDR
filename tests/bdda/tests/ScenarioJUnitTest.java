package bdda.tests;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.manager.DBManager;
import bdda.query.Condition;
import bdda.query.RelationScanner;
import bdda.query.RelationScannerWithSelect;
import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import bdda.storage.RecordId;
import bdda.storage.Relation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ScenarioJUnitTest {

    private DBConfig config;
    private DiskManager dm;
    private BufferManager bm;
    private DBManager dbm;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Configuration isolée pour chaque test
    	config = DBConfig.LoadDBConfig(new File("config/config.txt"));
        dm = new DiskManager(config);
        bm = new BufferManager(config, dm);
        dbm = new DBManager(config, dm, bm);
    }

    @AfterEach
    void tearDown() throws IOException {
        dbm.Finish();
    }

    @Test
    void testPartie1_TablePomme() throws IOException {
        // [1] CREATE TABLE Pomme
        List<ColumnInfo> cols = new ArrayList<>();
        cols.add(new ColumnInfo("C1", "INT"));
        cols.add(new ColumnInfo("C2", "VARCHAR(3)"));
        cols.add(new ColumnInfo("C3", "INT"));
        Relation pommeRel = new Relation("Pomme", cols, dm, bm);
        dbm.AddTable(pommeRel);

        // [2] INSERT
        pommeRel.InsertRecord(new Record(Arrays.asList(1, "aab", 2)));
        pommeRel.InsertRecord(new Record(Arrays.asList(2, "ab", 2)));
        pommeRel.InsertRecord(new Record(Arrays.asList(1, "agh", 1)));

        // [3] SELECT * FROM Pomme (Vérification du nombre)
        // Utilisation du Scanner pour compter
        RelationScanner scan1 = new RelationScanner(pommeRel);
        int count = 0;
        while (scan1.GetNextRecord() != null) count++;
        assertEquals(3, count, "Doit contenir 3 records");

        // [4] SELECT C2 FROM Pomme WHERE C1=1
        List<Condition> conds2 = new ArrayList<>();
        conds2.add(new Condition(0, null, Condition.OP_EQUAL, -1, 1));
        
        RelationScannerWithSelect scan2 = new RelationScannerWithSelect(pommeRel, conds2);
        List<String> resultsC2 = new ArrayList<>();
        Record r;
        while ((r = scan2.GetNextRecord()) != null) {
            resultsC2.add((String) r.getValue(1)); // Index 1 = C2
        }
        assertEquals(2, resultsC2.size());
        assertTrue(resultsC2.contains("aab"));
        assertTrue(resultsC2.contains("agh"));

        // [5] SELECT C1 FROM Pomme WHERE C3=1
        List<Condition> conds3 = new ArrayList<>();
        conds3.add(new Condition(2, null, Condition.OP_EQUAL, -1, 1));
        
        RelationScannerWithSelect scan3 = new RelationScannerWithSelect(pommeRel, conds3);
        int count3 = 0;
        while (scan3.GetNextRecord() != null) count3++;
        assertEquals(1, count3);

        // [6] DELETE WHERE C1=1 AND C3=2
        // Record cible : (1, "aab", 2)
        List<Condition> conds4 = new ArrayList<>();
        conds4.add(new Condition(0, null, Condition.OP_EQUAL, -1, 1));
        conds4.add(new Condition(2, null, Condition.OP_EQUAL, -1, 2));

        RelationScannerWithSelect scan4 = new RelationScannerWithSelect(pommeRel, conds4);
        int delCount = 0;
        while (scan4.GetNextRecord() != null) {
            pommeRel.DeleteRecord(scan4.GetCurrentRecordId());
            delCount++;
        }
        assertEquals(1, delCount, "1 record doit être supprimé");

        // Vérification post-delete (reste 2 records)
        assertEquals(2, pommeRel.GetAllRecords().size());

        // [7] UPDATE SET C1=3 WHERE C2 <= "ac"
        // Records restants : (2, "ab", 2) et (1, "agh", 1)
        // "ab" <= "ac" -> VRAI (Update)
        // "agh" <= "ac" -> FAUX (Pas update)
        
        List<Condition> conds5 = new ArrayList<>();
        conds5.add(new Condition(1, null, Condition.OP_LESS_EQUAL, -1, "ac"));

        RelationScannerWithSelect scan5 = new RelationScannerWithSelect(pommeRel, conds5);
        List<RecordId> toDelete = new ArrayList<>();
        List<Record> toInsert = new ArrayList<>();
        Record rec;

        while ((rec = scan5.GetNextRecord()) != null) {
            toDelete.add(scan5.GetCurrentRecordId());
            rec.setValue(0, 3); // Update C1
            toInsert.add(rec);
        }

        // Appliquer Update
        for (RecordId rid : toDelete) pommeRel.DeleteRecord(rid);
        for (Record rNew : toInsert) pommeRel.InsertRecord(rNew);

        assertEquals(1, toInsert.size(), "Seul 'ab' doit être mis à jour");
        
        // Vérification finale : on doit avoir un record avec C1=3
        boolean foundUpdated = false;
        for (Record check : pommeRel.GetAllRecords()) {
            if ((int)check.getValue(0) == 3 && check.getValue(1).equals("ab")) {
                foundUpdated = true;
            }
        }
        assertTrue(foundUpdated, "Le record mis à jour doit exister");
    }

    
    // Cette image aide à comprendre comment les Slots et la Bytemap (utilisés ci-dessous)
    // sont organisés physiquement sur le disque.

    @Test
    void testPartie2_TableS_CSV() throws IOException {
        // [A] Génération du CSV
        File csvFile = tempDir.resolve("S.csv").toFile();
        createDummySCsv(csvFile);

        // [8] CREATE TABLE S
        List<ColumnInfo> colsS = new ArrayList<>();
        colsS.add(new ColumnInfo("C1", "INT"));
        colsS.add(new ColumnInfo("C2", "FLOAT"));
        colsS.add(new ColumnInfo("C3", "INT"));
        colsS.add(new ColumnInfo("C4", "INT"));
        colsS.add(new ColumnInfo("C5", "INT"));
        
        Relation sRel = new Relation("S", colsS, dm, bm);
        dbm.AddTable(sRel);

        // [9] APPEND (Chargement manuel simulant SGBD)
        int appendCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                
                // Parsing manuel simplifié pour le test
                List<Object> values = parseCsvLine(line);
                sRel.InsertRecord(new Record(values));
                appendCount++;
            }
        }
        assertEquals(15, appendCount, "15 lignes doivent être insérées");

        // [10] SELECT * FROM S WHERE C3=12
        List<Condition> condsS = new ArrayList<>();
        condsS.add(new Condition(2, null, Condition.OP_EQUAL, -1, 12));

        RelationScannerWithSelect scanS = new RelationScannerWithSelect(sRel, condsS);
        int count12 = 0;
        while (scanS.GetNextRecord() != null) {
            count12++;
        }
        
        // Dans createDummySCsv, on crée 10 records avec C3=12
        assertEquals(10, count12);
    }

    
    // Cette image illustre comment le Scanner (itérateur) lit les pages une par une
    // lors des SELECT et UPDATE ci-dessus.

    // Helper pour créer le CSV
    private void createDummySCsv(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            for (int i = 0; i < 10; i++) {
                writer.write(i + ", " + (i * 1.5f) + ", 12, " + (i * 10) + ", " + (i * 100) + "\n");
            }
            for (int i = 0; i < 5; i++) {
                writer.write(i + ", " + (i * 2.5f) + ", 99, " + (i * 5) + ", " + (i * 50) + "\n");
            }
        }
    }

    // Helper simple pour parser le CSV (remplace SGBD.parseValues)
    private List<Object> parseCsvLine(String line) {
        String[] parts = line.split(",");
        List<Object> values = new ArrayList<>();
        values.add(Integer.parseInt(parts[0].trim())); // C1 INT
        values.add(Float.parseFloat(parts[1].trim())); // C2 FLOAT
        values.add(Integer.parseInt(parts[2].trim())); // C3 INT
        values.add(Integer.parseInt(parts[3].trim())); // C4 INT
        values.add(Integer.parseInt(parts[4].trim())); // C5 INT
        return values;
    }
}