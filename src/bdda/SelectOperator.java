package bdda;

import java.io.IOException;
import java.util.List;

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
        this.conditions = conditions;
        this.columns = columns;
    }

    @Override
    public Record GetNextRecord() throws IOException {
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
                return false;
            }
        }
        return true;
    }

    @Override
    public void Reset() throws IOException {
        child.Reset();
    }

    @Override
    public void Close() throws IOException {
        child.Close();
    }
}
