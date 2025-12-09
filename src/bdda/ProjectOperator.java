package bdda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements IRecordIterator {

    private final IRecordIterator child;
    private final List<Integer> projectionIndices;

    /**
     * @param child             opérateur fils (par ex. SelectOperator)
     * @param projectionIndices indices des colonnes à garder (0-based)
     */
    public ProjectOperator(IRecordIterator child, List<Integer> projectionIndices) {
        this.child = child;
        this.projectionIndices = new ArrayList<>(projectionIndices);
    }

    /**
     * Constructeur pratique pour SELECT * :
     * garde toutes les colonnes [0..columnCount-1].
     */
    public ProjectOperator(IRecordIterator child, int columnCount) {
        this.child = child;
        this.projectionIndices = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            this.projectionIndices.add(i);
        }
    }

    @Override
    public Record GetNextRecord() throws IOException {
        Record input = child.GetNextRecord();
        if (input == null) {
            return null;
        }

        Record projected = new Record();
        for (Integer idx : projectionIndices) {
            projected.addValue(input.getValue(idx));
        }
        return projected;
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

