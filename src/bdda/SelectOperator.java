package bdda;

import java.io.IOException;
import java.util.List;

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
        this.conditions = conditions;
        this.columns = columns;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        Record record;
        
        while ((record = childIterator.GetNextRecord()) != null) {
            // Verifier toutes les conditions (conjonction)
            if (evaluateAllConditions(record)) {
                return record;
            }
        }
        
        return null; // Plus de records qui satisfont les conditions
    }

}