package bdda.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import bdda.core.BufferManager;
import bdda.core.PageId;
import bdda.storage.ColumnInfo;
import bdda.storage.RecordId;
import bdda.storage.Relation;
import bdda.storage.Record;

public class RelationScannerWithSelect implements IRecordIterator {

    private final Relation relation;
    private final BufferManager bufferManager;
    private final List<Condition> conditions;
    private final List<ColumnInfo> columns;
    private final List<PageId> dataPages;

    // Curseurs
    private int pageCursor;
    private int slotCursor;
    
    // Pour les opérations UPDATE/DELETE
    private RecordId currentRecordId;

    public RelationScannerWithSelect(Relation relation, List<Condition> conditions) throws IOException {
        this.relation = relation;
        this.bufferManager = relation.getBufferManager();
        this.conditions = conditions;
        this.columns = relation.getColumns();
        this.dataPages = relation.getDataPages();
        this.pageCursor = 0;
        this.slotCursor = 0;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        while (pageCursor < dataPages.size()) {
            PageId currentPageId = dataPages.get(pageCursor);
            byte[] buffer = bufferManager.GetPage(currentPageId);
            ByteBuffer bb = ByteBuffer.wrap(buffer);

            int recordSize = relation.getRecordSize();
            int slotCount = relation.getSlotCount();
            int headerSize = 16;
            int bytemapOffset = headerSize + (slotCount * recordSize);

            while (slotCursor < slotCount) {

                if (bb.get(bytemapOffset + slotCursor) == 1) {
                    Record record = new Record();
                    int slotOffset = headerSize + (slotCursor * recordSize);
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    if (matchesAllConditions(record)) {
                        this.currentRecordId = new RecordId(currentPageId, slotCursor);
                        
                        slotCursor++;
                        bufferManager.FreePage(currentPageId, false);
                        return record;
                    }
                }
                slotCursor++;
            }

            bufferManager.FreePage(currentPageId, false);
            pageCursor++;
            slotCursor = 0;
        }
        return null;
    }
    
    // Retourne l'ID du dernier record lu (utile pour DELETE/UPDATE)
    public RecordId GetCurrentRecordId() {
        return currentRecordId;
    }

    private boolean matchesAllConditions(Record record) {
        if (conditions == null || conditions.isEmpty()) return true;
        for (Condition c : conditions) {
            if (!c.evaluate(record, columns)) return false;
        }
        return true;
    }

    @Override
    public void Reset() {
        this.pageCursor = 0;
        this.slotCursor = 0;
    }

    @Override
    public void Close() { }
}