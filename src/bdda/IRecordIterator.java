package bdda;

import java.io.IOException;

public interface IRecordIterator {

    Record GetNextRecord() throws IOException;

    void Reset() throws IOException;

    void Close() throws IOException;
}
