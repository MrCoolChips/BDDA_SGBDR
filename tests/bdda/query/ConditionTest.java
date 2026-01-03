package bdda.query;

import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConditionTest {

    // Helper pour créer un schéma de test : [0:INT, 1:FLOAT, 2:CHAR]
    private List<ColumnInfo> createSchema() {
        return Arrays.asList(
            new ColumnInfo("Age", "INT"),
            new ColumnInfo("Moyenne", "FLOAT"),
            new ColumnInfo("Nom", "CHAR(10)")
        );
    }

    @Test
    void testIntEqual() {
        List<ColumnInfo> schema = createSchema();
        
        // Condition : Age = 20
        Condition cond = new Condition(0, null, Condition.OP_EQUAL, -1, 20);

        // Cas : Vrai
        Record r1 = new Record(Arrays.asList(20, 10.5f, "Alice"));
        assertTrue(cond.evaluate(r1, schema));

        // Cas : Faux
        Record r2 = new Record(Arrays.asList(18, 10.5f, "Bob"));
        assertFalse(cond.evaluate(r2, schema));
    }

    @Test
    void testFloatComparison() {
        List<ColumnInfo> schema = createSchema();

        // Condition : Moyenne > 10.0
        Condition cond = new Condition(1, null, Condition.OP_GREATER, -1, 10.0f);

        // 12.5 > 10.0 -> Vrai
        assertTrue(cond.evaluate(new Record(Arrays.asList(20, 12.5f, "A")), schema));
        
        // 9.5 > 10.0 -> Faux
        assertFalse(cond.evaluate(new Record(Arrays.asList(20, 9.5f, "B")), schema));
    }

    @Test
    void testStringComparison() {
        List<ColumnInfo> schema = createSchema();

        // Condition : Nom = "Zoro"
        Condition cond = new Condition(2, null, Condition.OP_EQUAL, -1, "Zoro");

        assertTrue(cond.evaluate(new Record(Arrays.asList(20, 10f, "Zoro")), schema));
        assertFalse(cond.evaluate(new Record(Arrays.asList(20, 10f, "Luffy")), schema));
    }

    @Test
    void testColToColComparison() {
        // Schéma : [0:INT (Val1), 1:INT (Val2)]
        List<ColumnInfo> schema = Arrays.asList(
            new ColumnInfo("Val1", "INT"),
            new ColumnInfo("Val2", "INT")
        );

        // Condition : Val1 < Val2
        Condition cond = new Condition(0, null, Condition.OP_LESS, 1, null);

        // 10 < 20 -> Vrai
        assertTrue(cond.evaluate(new Record(Arrays.asList(10, 20)), schema));

        // 30 < 20 -> Faux
        assertFalse(cond.evaluate(new Record(Arrays.asList(30, 20)), schema));
    }

    @Test
    void testTypeConversion() {
        List<ColumnInfo> schema = createSchema();

        // Condition : Age (INT) = "25" (String)
        // Le système doit convertir "25" en 25
        Condition cond = new Condition(0, null, Condition.OP_EQUAL, -1, "25");

        assertTrue(cond.evaluate(new Record(Arrays.asList(25, 0f, "X")), schema));
    }

    @Test
    void testOperatorsLimit() {
        List<ColumnInfo> schema = createSchema();
        Record rec = new Record(Arrays.asList(10, 10f, "X")); // Valeur 10

        // Test >=
        Condition gte = new Condition(0, null, Condition.OP_GREATER_EQUAL, -1, 10);
        assertTrue(gte.evaluate(rec, schema));

        // Test <=
        Condition lte = new Condition(0, null, Condition.OP_LESS_EQUAL, -1, 10);
        assertTrue(lte.evaluate(rec, schema));

        // Test <>
        Condition neq = new Condition(0, null, Condition.OP_NOT_EQUAL, -1, 5);
        assertTrue(neq.evaluate(rec, schema));
    }
}