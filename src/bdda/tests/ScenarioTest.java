package bdda.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.manager.DBManager;
import bdda.query.Condition;
import bdda.query.ProjectOperator;
import bdda.query.RecordPrinter;
import bdda.query.RelationScanner;
import bdda.query.RelationScannerWithSelect;
import bdda.storage.ColumnInfo;
import bdda.storage.Relation;
import bdda.storage.Record;
import bdda.storage.RecordId;

// SGBD sınıfındaki statik metotları kullanmak için gerekli referans
// (Eğer SGBD sınıfı bdda.sgbd paketindeyse import gerekebilir veya tam yol yazılır)
import bdda.sgbd.SGBD; 

public class ScenarioTest {

    public static void main(String[] args) {
        try {
            System.out.println("=== TEST SCENARIO ===");
            
            // ---------------------------------------------------------
            // 0. INITIALISATION
            // ---------------------------------------------------------
            File config = new File("config/config.txt");
            DBConfig bd = DBConfig.LoadDBConfig(config);
            DiskManager dm = new DiskManager(bd);
            BufferManager bm = new BufferManager(bd, dm);
            DBManager dbm = new DBManager(bd, dm, bm);
            
            dbm.RemoveAllTables();

            // =========================================================
            // PARTIE 1 : TABLE POMME
            // =========================================================

            // 1. CREATE TABLE Pomme (C1:INT,C2:VARCHAR(3),C3:INT)
            System.out.println("\n[1] CREATE TABLE Pomme (C1:INT,C2:VARCHAR(3),C3:INT)");
            List<ColumnInfo> colsPomme = new ArrayList<>();
            colsPomme.add(new ColumnInfo("C1", "INT"));
            colsPomme.add(new ColumnInfo("C2", "VARCHAR(3)"));
            colsPomme.add(new ColumnInfo("C3", "INT"));
            Relation pommeRel = new Relation("Pomme", colsPomme, dm, bm);
            dbm.AddTable(pommeRel);

            // 2. INSERT INTO Pomme ...
            System.out.println("[2] INSERT INTO Pomme VALUES ...");
            pommeRel.InsertRecord(new Record(Arrays.asList(1, "aab", 2)));
            pommeRel.InsertRecord(new Record(Arrays.asList(2, "ab", 2)));
            pommeRel.InsertRecord(new Record(Arrays.asList(1, "agh", 1)));
            System.out.println("-> Tuples insérés.");

            // 3. SELECT * FROM Pomme p
            System.out.println("\n[3] SELECT * FROM Pomme p");
            System.out.println("Attendu : 3 records");
            System.out.println("--- Résultat ---");
            RelationScanner scan1 = new RelationScanner(pommeRel);
            ProjectOperator proj1 = new ProjectOperator(scan1, 3);
            new RecordPrinter(proj1).printAll();

            // 4. SELECT pp.C2 FROM Pomme pp WHERE pp.C1=1
            System.out.println("\n[4] SELECT pp.C2 FROM Pomme pp WHERE pp.C1=1");
            System.out.println("Attendu : aab, agh (Total=2)");
            System.out.println("--- Résultat ---");
            // Parsing manuel: C1 -> Index 0, C2 -> Index 1
            List<Condition> conds2 = new ArrayList<>();
            conds2.add(new Condition(
                0,                // leftColIndex (C1)
                null,             // leftConstant (null car colonne)
                Condition.OP_EQUAL,
                -1,               // rightColIndex (-1 car constante)
                1                 // rightConstant
            ));
            RelationScannerWithSelect scan2 = new RelationScannerWithSelect(pommeRel, conds2);
            ProjectOperator proj2 = new ProjectOperator(scan2, Arrays.asList(1));
            new RecordPrinter(proj2).printAll();

            // 5. SELECT pote.C1,pote.C1 FROM Pomme pote WHERE pote.C3=1
            System.out.println("\n[5] SELECT pote.C1,pote.C1 FROM Pomme pote WHERE pote.C3=1");
            System.out.println("Attendu : 1 ; 1 (Total=1)");
            System.out.println("--- Résultat ---");
            // Parsing manuel: C3 -> Index 2
            List<Condition> conds3 = new ArrayList<>();
            conds3.add(new Condition(
                2,                // C3
                null,
                Condition.OP_EQUAL,
                -1,
                1
            ));
            RelationScannerWithSelect scan3 = new RelationScannerWithSelect(pommeRel, conds3);
            ProjectOperator proj3 = new ProjectOperator(scan3, Arrays.asList(0, 0));
            new RecordPrinter(proj3).printAll();

            // 6. DELETE Pomme c WHERE c.C1=1 AND c.C3=2
            System.out.println("\n[6] DELETE Pomme c WHERE c.C1=1 AND c.C3=2");
            System.out.println("Attendu : Total deleted records = 1");
            System.out.println("--- Résultat ---");
            // Parsing manuel: C1 -> Index 0, C3 -> Index 2
            List<Condition> conds4 = new ArrayList<>();
            conds4.add(new Condition(
                0,
                null,
                Condition.OP_EQUAL,
                -1,
                1
            ));
            conds4.add(new Condition(
                2,
                null,
                Condition.OP_EQUAL,
                -1,
                2
            ));
            
            RelationScannerWithSelect scan4 = new RelationScannerWithSelect(pommeRel, conds4);
            int delCount = 0;
            while (scan4.GetNextRecord() != null) {
                pommeRel.DeleteRecord(scan4.GetCurrentRecordId());
                delCount++;
            }
            System.out.println("Total deleted records = " + delCount);

            // 7. UPDATE Pomme p SET p.C1=3 WHERE p.C2<\"ac\"
            System.out.println("\n[7] UPDATE Pomme p SET p.C1=3 WHERE p.C2<\"ac\"");
            System.out.println("Attendu : Total updated records = 1");
            System.out.println("--- Résultat ---");
            // Parsing manuel: C2 -> Index 1, C1 -> Index 0
            List<Condition> conds5 = new ArrayList<>();
            conds5.add(new Condition(
                1,
                null,
                Condition.OP_LESS_EQUAL, // or OP_LESS si istersen tam olarak '<'
                -1,
                "ac"
            ));
            
            RelationScannerWithSelect scan5 = new RelationScannerWithSelect(pommeRel, conds5);
            List<RecordId> toDelete = new ArrayList<>();
            List<Record> toInsert = new ArrayList<>();
            Record rec;
            
            while ((rec = scan5.GetNextRecord()) != null) {
                toDelete.add(scan5.GetCurrentRecordId());
                // Modification: Set C1 (Idx 0) = 3
                rec.setValue(0, 3);
                toInsert.add(rec);
            }
            // Appliquer Update
            for (RecordId rid : toDelete) {
                pommeRel.DeleteRecord(rid);
            }
            for (Record r : toInsert) {
                pommeRel.InsertRecord(r);
            }
            
            System.out.println("Total updated records = " + toInsert.size());


            // =========================================================
            // PARTIE 2 : TABLE S & CSV
            // =========================================================

            // A. Préparation du fichier S.csv (Si non existant)
            // On génère un fichier avec 15 lignes, dont 10 où C3=12
            File csvFile = new File("S.csv");
            createDummySCsv(csvFile);

            // 8. CREATE TABLE S (C1:INT,C2:REAL,C3:INT,C4:INT,C5:INT)
            System.out.println("\n[8] CREATE TABLE S (C1:INT,C2:FLOAT,C3:INT,C4:INT,C5:INT)");
            List<ColumnInfo> colsS = new ArrayList<>();
            colsS.add(new ColumnInfo("C1", "INT"));
            colsS.add(new ColumnInfo("C2", "FLOAT")); // REAL = FLOAT
            colsS.add(new ColumnInfo("C3", "INT"));
            colsS.add(new ColumnInfo("C4", "INT"));
            colsS.add(new ColumnInfo("C5", "INT"));
            
            Relation sRel = new Relation("S", colsS, dm, bm);
            dbm.AddTable(sRel);
            System.out.println("Table S créée.");

            // 9. APPEND INTO S ALLRECORDS(S.csv)
            System.out.println("\n[9] APPEND INTO S ALLRECORDS(S.csv)");
            System.out.println("Traitement de la commande d'insertion de masse...");

            // Simulation de la commande reçue par le SGBD
            String command = "APPEND INTO S ALLRECORDS (S.csv)";
            
            // Extraction des paramètres de la commande
            String rest = command.substring(12);
            int allrecordsPos = rest.indexOf(" ALLRECORDS ");
            String tableName = rest.substring(0, allrecordsPos).trim();
            
            // Extraction du nom de fichier
            String filePart = rest.substring(allrecordsPos + 12).trim();
            String fileName = filePart.substring(1, filePart.length() - 1);
            
            // Récupération de la relation
            Relation relation = dbm.GetTable(tableName);
            
            if (relation == null) {
                System.out.println("Erreur : Table inexistante (" + tableName + ")");
            } else {
                File fileToRead = new File(fileName);
                if (!fileToRead.exists()) {
                    System.out.println("Erreur : Fichier introuvable (" + fileName + ")");
                } else {
                    int appendCount = 0;
                    try (BufferedReader reader = new BufferedReader(new FileReader(fileToRead))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.trim().isEmpty()) {
                                continue;
                            }
                            
                            // Utilisation directe de la méthode statique SGBD.parseValues
                            // pour parser la ligne CSV et convertir les types
                            List<Object> values = SGBD.parseValues(line, relation.getColumns());
                            
                            Record record = new Record(values);
                            relation.InsertRecord(record);
                            appendCount++;
                        }
                    }
                    System.out.println("-> " + appendCount + " tuples insérés depuis " + fileName + ".");
                }
            }

            // 10. SELECT * FROM S s WHERE s.C3=12
            System.out.println("\n[10] SELECT * FROM S s WHERE s.C3=12");
            System.out.println("Attendu : 10 tuples (environ, selon le CSV généré)");
            System.out.println("--- Résultat ---");
            
            // Parsing manuel: C3 -> Index 2
            List<Condition> condsS = new ArrayList<>();
            condsS.add(new Condition(
                2,
                null,
                Condition.OP_EQUAL,
                -1,
                12
            ));
            
            RelationScannerWithSelect scanS = new RelationScannerWithSelect(sRel, condsS);
            ProjectOperator projS = new ProjectOperator(scanS, 5); // 5 colonnes
            new RecordPrinter(projS).printAll();

            // FIN
            dbm.Finish();
            // Suppression du CSV de test
            if (csvFile.exists()) {
                if (csvFile.delete()) {
                    // Supprimé avec succès
                } else {
                    System.out.println("Impossible de supprimer S.csv.");
                }
            }
            System.out.println("\n=== TEST TERMINÉ ===");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Génère un fichier S.csv pour le test si nécessaire
     * Contenu : 15 lignes au total
     * - 10 lignes avec C3 = 12
     * - 5 lignes avec C3 != 12
     */
    private static void createDummySCsv(File file) {
        try (FileWriter writer = new FileWriter(file)) {
            // Générer 10 records valides (C3=12)
            for (int i = 0; i < 10; i++) {
                // C1, C2, C3=12, C4, C5
                writer.write(i + ", " + (i * 1.5f) + ", 12, " + (i * 10) + ", " + (i * 100) + "\n");
            }
            // Générer 5 records "bruit" (C3!=12)
            for (int i = 0; i < 5; i++) {
                writer.write(i + ", " + (i * 2.5f) + ", 99, " + (i * 5) + ", " + (i * 50) + "\n");
            }
            System.out.println("(Fichier S.csv généré pour le test)");
        } catch (IOException e) {
            System.err.println("Impossible de créer S.csv");
        }
    }
}