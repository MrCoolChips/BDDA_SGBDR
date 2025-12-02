package bdda;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Tests pour les classes Record et Relation
 */
public class RelationTests {
    
    public static void main(String[] args) {
        System.out.println("=== TESTS RELATION ET RECORD (TP4) ===\n");
        
        try {
            testColumnInfo();
            testBasicWriteRead();
            testAllTypes();
            testVarchar();
            testMultipleRecords();
            testEdgeCases();
            
            System.out.println("\n=== TOUS LES TESTS RÉUSSIS ! ===");
            
        } catch (Exception e) {
            System.err.println("ERREUR : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test de la classe ColumnInfo
     */
    private static void testColumnInfo() {
        System.out.println("1. Test ColumnInfo...");
        
        ColumnInfo colInt = new ColumnInfo("id", "INT");
        ColumnInfo colFloat = new ColumnInfo("prix", "FLOAT");
        ColumnInfo colChar = new ColumnInfo("code", "CHAR(10)");
        ColumnInfo colVarchar = new ColumnInfo("desc", "VARCHAR(20)");
        
        // Vérifier les tailles
        assert colInt.getSizeInBytes() == 4 : "INT devrait faire 4 bytes";
        assert colFloat.getSizeInBytes() == 4 : "FLOAT devrait faire 4 bytes";
        assert colChar.getSizeInBytes() == 10 : "CHAR(10) devrait faire 10 bytes";
        assert colVarchar.getSizeInBytes() == 24 : "VARCHAR(20) devrait faire 24 bytes (4+20)";
        
        // Vérifier les types
        assert colInt.isInt() : "colInt devrait être INT";
        assert colFloat.isFloat() : "colFloat devrait être FLOAT";
        assert colChar.isChar() : "colChar devrait être CHAR";
        assert colVarchar.isVarchar() : "colVarchar devrait être VARCHAR";
        
        System.out.println("   OK - ColumnInfo fonctionne correctement");
        System.out.println("   INT: " + colInt.getSizeInBytes() + " bytes");
        System.out.println("   FLOAT: " + colFloat.getSizeInBytes() + " bytes");
        System.out.println("   CHAR(10): " + colChar.getSizeInBytes() + " bytes");
        System.out.println("   VARCHAR(20): " + colVarchar.getSizeInBytes() + " bytes");
    }
    
    /**
     * Test basique écriture/lecture
     */
    private static void testBasicWriteRead() {
        System.out.println("\n2. Test basique écriture/lecture...");
        
        // Créer une relation simple
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        Relation rel = new Relation("Test", cols);
        
        System.out.println("   " + rel);
        
        // Créer un record
        Record r1 = new Record(Arrays.asList(42, "Alice"));
        
        // Écrire dans un buffer
        ByteBuffer buff = ByteBuffer.allocate(rel.getRecordSize());
        rel.writeRecordToBuffer(r1, buff, 0);
        
        // Lire depuis le buffer
        Record r2 = new Record();
        rel.readFromBuffer(r2, buff, 0);
        
        // Vérifier
        assert r2.getValue(0).equals(42) : "ID incorrect : " + r2.getValue(0);
        assert r2.getValue(1).equals("Alice") : "Nom incorrect : " + r2.getValue(1);
        
        System.out.println("   OK - Record original : " + r1);
        System.out.println("   OK - Record lu       : " + r2);
    }
    
    /**
     * Test tous les types
     */
    private static void testAllTypes() {
        System.out.println("\n3. Test tous les types (INT, FLOAT, CHAR, VARCHAR)...");
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("note", "FLOAT"),
            new ColumnInfo("code", "CHAR(5)"),
            new ColumnInfo("description", "VARCHAR(20)")
        );
        Relation rel = new Relation("Produits", cols);
        
        System.out.println("   " + rel);
        
        // Créer un record avec tous les types
        Record r1 = new Record(Arrays.asList(
            123,           // INT
            15.75f,        // FLOAT
            "ABC",         // CHAR(5)
            "Hello World"  // VARCHAR(20)
        ));
        
        // Écrire et lire
        ByteBuffer buff = ByteBuffer.allocate(rel.getRecordSize());
        rel.writeRecordToBuffer(r1, buff, 0);
        
        Record r2 = new Record();
        rel.readFromBuffer(r2, buff, 0);
        
        System.out.println("   OK - Record original : " + r1);
        System.out.println("   OK - Record lu       : " + r2);
        
        // Vérifier chaque valeur
        assert r2.getValue(0).equals(123) : "INT incorrect";
        assert r2.getValue(1).equals(15.75f) : "FLOAT incorrect";
        assert r2.getValue(2).equals("ABC") : "CHAR incorrect";
        assert r2.getValue(3).equals("Hello World") : "VARCHAR incorrect";
        
        System.out.println("   OK - Tous les types correctement écrits/lus");
    }
    
    /**
     * Test VARCHAR spécifiquement
     */
    private static void testVarchar() {
        System.out.println("\n4. Test VARCHAR avec différentes longueurs...");
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("texte", "VARCHAR(50)")
        );
        Relation rel = new Relation("Textes", cols);
        
        // Test avec différentes longueurs
        String[] testStrings = {"", "A", "Hello", "Ceci est un texte plus long"};
        
        for (String str : testStrings) {
            Record r1 = new Record(Arrays.asList(str));
            
            ByteBuffer buff = ByteBuffer.allocate(rel.getRecordSize());
            rel.writeRecordToBuffer(r1, buff, 0);
            
            Record r2 = new Record();
            rel.readFromBuffer(r2, buff, 0);
            
            assert r2.getValue(0).equals(str) : "VARCHAR incorrect pour : '" + str + "'";
            System.out.println("   OK - VARCHAR '" + str + "' (longueur " + str.length() + ")");
        }
    }
    
    /**
     * Test plusieurs records dans un même buffer
     */
    private static void testMultipleRecords() {
        System.out.println("\n5. Test plusieurs records dans un buffer...");
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        Relation rel = new Relation("Personnes", cols);
        
        int recordSize = rel.getRecordSize();
        System.out.println("   Taille d'un record : " + recordSize + " bytes");
        
        // Créer plusieurs records
        Record[] records = {
            new Record(Arrays.asList(1, "Alice", 22)),
            new Record(Arrays.asList(2, "Bob", 25)),
            new Record(Arrays.asList(3, "Charlie", 30))
        };
        
        // Écrire tous les records dans un buffer
        ByteBuffer buff = ByteBuffer.allocate(recordSize * records.length);
        
        for (int i = 0; i < records.length; i++) {
            rel.writeRecordToBuffer(records[i], buff, i * recordSize);
        }
        
        // Lire tous les records
        for (int i = 0; i < records.length; i++) {
            Record r = new Record();
            rel.readFromBuffer(r, buff, i * recordSize);
            
            System.out.println("   OK - Record " + i + " : " + r);
            
            // Vérifier
            assert r.getValue(0).equals(records[i].getValue(0)) : "ID incorrect";
            assert r.getValue(1).equals(records[i].getValue(1)) : "Nom incorrect";
            assert r.getValue(2).equals(records[i].getValue(2)) : "Age incorrect";
        }
        
        System.out.println("   OK - Tous les records correctement écrits/lus");
    }
    
    /**
     * Test des cas limites
     */
    private static void testEdgeCases() {
        System.out.println("\n6. Test cas limites...");
        
        // Test avec chaîne trop longue (doit être tronquée)
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("code", "CHAR(5)")
        );
        Relation rel = new Relation("Codes", cols);
        
        Record r1 = new Record(Arrays.asList("ABCDEFGHIJ")); // 10 chars pour CHAR(5)
        
        ByteBuffer buff = ByteBuffer.allocate(rel.getRecordSize());
        rel.writeRecordToBuffer(r1, buff, 0);
        
        Record r2 = new Record();
        rel.readFromBuffer(r2, buff, 0);
        
        assert r2.getValue(0).equals("ABCDE") : "Troncature CHAR incorrecte";
        System.out.println("   OK - CHAR(5) avec 'ABCDEFGHIJ' → '" + r2.getValue(0) + "' (tronqué)");
        
        // Test avec conversion String → INT
        cols = Arrays.asList(new ColumnInfo("id", "INT"));
        rel = new Relation("Test", cols);
        
        r1 = new Record(Arrays.asList("42")); // String au lieu de int
        
        buff = ByteBuffer.allocate(rel.getRecordSize());
        rel.writeRecordToBuffer(r1, buff, 0);
        
        r2 = new Record();
        rel.readFromBuffer(r2, buff, 0);
        
        assert r2.getValue(0).equals(42) : "Conversion String→INT incorrecte";
        System.out.println("   OK - Conversion '42' (String) → 42 (INT)");
        
        // Test avec conversion String → FLOAT
        cols = Arrays.asList(new ColumnInfo("prix", "FLOAT"));
        rel = new Relation("Test", cols);
        
        r1 = new Record(Arrays.asList("3.14")); // String au lieu de float
        
        buff = ByteBuffer.allocate(rel.getRecordSize());
        rel.writeRecordToBuffer(r1, buff, 0);
        
        r2 = new Record();
        rel.readFromBuffer(r2, buff, 0);
        
        assert Math.abs((Float) r2.getValue(0) - 3.14f) < 0.001 : "Conversion String→FLOAT incorrecte";
        System.out.println("   OK - Conversion '3.14' (String) → 3.14 (FLOAT)");
    }
}