package bdda;

import java.io.IOException;
import java.util.List;

public class RecordPrinter {

    private final IRecordIterator iterator;

    public RecordPrinter(IRecordIterator iterator) {
        this.iterator = iterator;
    }

    public void Print() throws IOException {
        int count = 0;
        Record record;
        
        // Boucle sur l'itérateur
        while ((record = iterator.GetNextRecord()) != null) {
            StringBuilder sb = new StringBuilder();
            List<Object> values = record.getValues();
            
            for (int i = 0; i < values.size(); i++) {
                sb.append(values.get(i).toString());
                if (i < values.size() - 1) {
                    sb.append(" ; ");
                }
            }
            sb.append("."); // Point à la fin
            System.out.println(sb.toString());
            count++;
        }
        
        System.out.println("Total selected records = " + count);
        
        // Fermer l'itérateur
        iterator.Close();
    }
}