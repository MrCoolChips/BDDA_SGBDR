package bdda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


public class RelationScanner implements IRecordIterator {

    private final Relation relation;
    private final BufferManager bufferManager;
    
    // Liste des IDs de pages à parcourir
    private final List<PageId> dataPages;
    
    private int pageCursor; // Index dans la liste dataPages
    private int slotCursor; // Index du slot dans la page courante

    public RelationScanner(Relation relation) throws IOException {
        this.relation = relation;
        this.bufferManager = relation.getBufferManager();
        
        // On récupère juste la liste des IDs, pas les données
        this.dataPages = relation.getDataPages();
        
        this.pageCursor = 0;
        this.slotCursor = 0;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        // Tant qu'il reste des pages à parcourir
        while (pageCursor < dataPages.size()) {
            
            PageId currentPageId = dataPages.get(pageCursor);
            byte[] buffer = bufferManager.GetPage(currentPageId);
            ByteBuffer bb = ByteBuffer.wrap(buffer);

            // Calculer les offsets nécessaires pour lire la Bytemap
            int recordSize = relation.getRecordSize();
            int slotCount = relation.getSlotCount();
            int headerSize = 16; // DATA_PAGE_HEADER_SIZE (prev+next)
            int bytemapOffset = headerSize + (slotCount * recordSize);

            // Parcourir les slots restants de la page courante
            while (slotCursor < slotCount) {
                // Vérifier dans la Bytemap si le slot est occupé (1) ou vide (0)
                if (bb.get(bytemapOffset + slotCursor) == 1) {
                    
                    // Slot occupé -> On lit le record
                    Record record = new Record();
                    int slotOffset = headerSize + (slotCursor * recordSize);
                    
                    // On lit depuis le buffer (sans créer d'objet intermédiaire lourd)
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    slotCursor++;
                    
                    // On libère la page avant de retourner
                    bufferManager.FreePage(currentPageId, false);
                    
                    return record;
                }
                
                slotCursor++;
            }

            // Si on arrive ici, c'est qu'on a fini la page courante
            bufferManager.FreePage(currentPageId, false);
            
            pageCursor++;
            slotCursor = 0;
            
        }

        return null;
    }

    @Override
    public void Reset() {
        this.pageCursor = 0;
        this.slotCursor = 0;
    }

    @Override
    public void Close() {
        // les pages sont libérées à chaque return, donc rien à faire ici.
    }
}