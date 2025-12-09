package bdda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

<<<<<<< HEAD

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
=======
/**
 * Iterateur qui parcourt tous les records d'une relation
 * Implementation efficace : ne garde qu'un record a la fois en memoire
 */
public class RelationScanner implements IRecordIterator {
    
    private Relation relation;
    private BufferManager bufferManager;
    
    // Liste des pages de donnees
    private List<PageId> dataPages;
    private int currentPageIndex;
    private int currentSlotIndex;
    
    // Page courante en memoire
    private PageId currentPageId;
    private byte[] currentBuffer;
    
    // Constantes
    private static final int DATA_PAGE_HEADER_SIZE = 16;
    
    public RelationScanner(Relation relation, BufferManager bufferManager) throws IOException {
        this.relation = relation;
        this.bufferManager = bufferManager;
        this.dataPages = relation.getDataPages();
        this.currentPageIndex = 0;
        this.currentSlotIndex = 0;
        this.currentPageId = null;
        this.currentBuffer = null;
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
    }

    @Override
    public Record GetNextRecord() throws IOException {
<<<<<<< HEAD
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
=======
        int slotCount = relation.getSlotCount();
        int bytemapOffset = DATA_PAGE_HEADER_SIZE + (slotCount * relation.getRecordSize());
        
        while (currentPageIndex < dataPages.size()) {
            // Charger la page si necessaire
            if (currentPageId == null || !currentPageId.equals(dataPages.get(currentPageIndex))) {
                // Liberer l'ancienne page
                if (currentPageId != null) {
                    bufferManager.FreePage(currentPageId, false);
                }
                
                currentPageId = dataPages.get(currentPageIndex);
                currentBuffer = bufferManager.GetPage(currentPageId);
            }
            
            ByteBuffer bb = ByteBuffer.wrap(currentBuffer);
            
            // Chercher le prochain slot occupe
            while (currentSlotIndex < slotCount) {
                if (bb.get(bytemapOffset + currentSlotIndex) == 1) {
                    // Slot occupe : lire le record
                    Record record = new Record();
                    int slotOffset = DATA_PAGE_HEADER_SIZE + (currentSlotIndex * relation.getRecordSize());
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    currentSlotIndex++;
                    return record;
                }
                currentSlotIndex++;
            }
            
            // Page terminee, passer a la suivante
            currentSlotIndex = 0;
            currentPageIndex++;
        }
        
        return null; // Plus de records
    }

     @Override
    public void Close() {
        if (currentPageId != null) {
            bufferManager.FreePage(currentPageId, false);
            currentPageId = null;
            currentBuffer = null;
        }
    }

    @Override
    public void Reset() throws IOException {
        Close();
        this.dataPages = relation.getDataPages();
        this.currentPageIndex = 0;
        this.currentSlotIndex = 0;
    }
    
}
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
