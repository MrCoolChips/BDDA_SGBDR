package bdda;

import java.io.IOException;

<<<<<<< HEAD
public interface IRecordIterator {

    Record GetNextRecord() throws IOException;

    void Reset() throws IOException;

    void Close() throws IOException;
}
=======
/**
 * Interface pour parcourir un ensemble de records
 */
public interface IRecordIterator {
    
    /**
     * Retourne le prochain record et avance le curseur
     * @return le prochain record, ou null s'il n'y en a plus
     */
    Record GetNextRecord() throws IOException;
    
    /**
     * Ferme l'iterateur et libere les ressources
     */
    void Close();
    
    /**
     * Remet le curseur au debut
     */
    void Reset() throws IOException;
}
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
