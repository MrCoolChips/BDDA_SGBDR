package bdda.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bdda.storage.Record;

/**
 * Operateur de projection (selectionne certaines colonnes)
 */
public class ProjectOperator implements IRecordIterator {

    private final IRecordIterator childIterator;
    // null => pas de projection (renvoyer le record tel quel)
    private final List<Integer> columnIndices;

    /**
     * @param childIterator iterateur fils
     * @param columnIndices indices des colonnes a projeter
     *                      (null = aucune projection, on garde tout)
     */
    public ProjectOperator(IRecordIterator childIterator, List<Integer> columnIndices) {
        this.childIterator = childIterator;
        this.columnIndices = (columnIndices != null) ? new ArrayList<>(columnIndices): null;
    }

    /**
     * Constructeur pratique pour SELECT * :
     * garde toutes les colonnes [0..columnCount-1].
     */
    public ProjectOperator(IRecordIterator childIterator, int columnCount) {
        this.childIterator = childIterator;
        this.columnIndices = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            this.columnIndices.add(i);
        }
    }

    @Override
    public Record GetNextRecord() throws IOException {
        Record input = childIterator.GetNextRecord();
        if (input == null) {
            return null;
        }

        // Pas de projection specifique -> on renvoie le record tel quel
        if (columnIndices == null) {
            return input;
        }

        // Construire un nouveau record avec les colonnes projetees
        Record projected = new Record();
        for (Integer idx : columnIndices) {
            projected.addValue(input.getValue(idx));
        }
        return projected;
    }

    @Override
    public void Close() {
        childIterator.Close();
    }

    @Override
    public void Reset() throws IOException {
        childIterator.Reset();
    }
}