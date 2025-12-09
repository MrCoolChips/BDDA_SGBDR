package bdda;

import java.io.IOException;
import java.util.List;

<<<<<<< HEAD
public class SelectOperator implements IRecordIterator {

    private final IRecordIterator child;
    private final List<Condition> conditions;
    private final List<ColumnInfo> columns;

    /**
     * @param child      opérateur fils (par ex. un RelationScanner)
     * @param conditions liste de conditions C_i du WHERE
     * @param columns    schéma de la relation (pour connaître les types)
     */
    public SelectOperator(IRecordIterator child, List<Condition> conditions, List<ColumnInfo> columns) {
        this.child = child;
=======
/**
 * Operateur de selection (filtre les records selon des conditions)
 */
public class SelectOperator implements IRecordIterator {
    
    private IRecordIterator childIterator;
    private List<Condition> conditions;
    private List<ColumnInfo> columns;
    
    public SelectOperator(IRecordIterator childIterator, 
                          List<Condition> conditions,
                          List<ColumnInfo> columns) {
        this.childIterator = childIterator;
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
        this.conditions = conditions;
        this.columns = columns;
    }

    @Override
    public Record GetNextRecord() throws IOException {
<<<<<<< HEAD
        while (true) {
            Record r = child.GetNextRecord();
            if (r == null) {
                // Plus de tuples chez l'opérateur fils
                return null;
            }

            if (matchesAllConditions(r)) {
                return r;
            }
            // sinon, on continue à chercher
        }
    }

    private boolean matchesAllConditions(Record record) {
        if (conditions == null || conditions.isEmpty()) {
            // Pas de WHERE -> tout passe
            return true;
        }
        for (Condition c : conditions) {
            if (!c.evaluate(record, columns)) {
=======
        Record record;
        
        while ((record = childIterator.GetNextRecord()) != null) {
            // Verifier toutes les conditions (conjonction)
            if (evaluateAllConditions(record)) {
                return record;
            }
        }
        
        return null; // Plus de records qui satisfont les conditions
    }

    /**
     * Evalue toutes les conditions (AND)
     */
    private boolean evaluateAllConditions(Record record) {
        for (Condition cond : conditions) {
            if (!cond.evaluate(record, columns)) {
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
                return false;
            }
        }
        return true;
    }

    @Override
<<<<<<< HEAD
    public void Reset() throws IOException {
        child.Reset();
    }

    @Override
    public void Close() throws IOException {
        child.Close();
    }
}
=======
    public void Close() {
        childIterator.Close();
    }
    
    @Override
    public void Reset() throws IOException {
        childIterator.Reset();
    }

}
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
