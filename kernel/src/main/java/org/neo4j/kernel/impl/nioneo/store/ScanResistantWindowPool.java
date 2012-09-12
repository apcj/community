package org.neo4j.kernel.impl.nioneo.store;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ScanResistantWindowPool implements WindowPool
{
    private final String storeFileName;
    private final FileChannel fileChannel;
    private final int bytesPerRecord;
    private final int recordsPerPage;
//    Page[] pages;
//    Cart cart;

    public ScanResistantWindowPool( String storeFileName, int bytesPerRecord, int targetBytesPerPage, FileChannel fileChannel )
    {
        this.storeFileName = storeFileName;
        this.bytesPerRecord = bytesPerRecord;
        this.fileChannel = fileChannel;
        this.recordsPerPage = calculateNumberOfRecordsPerPage( bytesPerRecord, targetBytesPerPage );
    }

    private static int calculateNumberOfRecordsPerPage( int bytesPerRecord, int targetBytesPerPage )
    {
        if ( bytesPerRecord <= 0 || bytesPerRecord > targetBytesPerPage )
        {
            throw new IllegalArgumentException( String.format( "number of bytes per record [%d] " +
                    "is not in the valid range [1-%d]", bytesPerRecord, targetBytesPerPage ) );
        }
        return targetBytesPerPage / bytesPerRecord;
    }

    @Override
    public PersistenceWindow acquire( long position, OperationType operationType )
    {
        if ( operationType != OperationType.READ )
        {
            throw new UnsupportedOperationException( "Only supports READ operations." );
        }
        try
        {
            int pageNumber = (int) position / recordsPerPage;
//            cart.acquire( pages[pageNumber], this );
            return new MappedWindow( recordsPerPage, bytesPerRecord, pageNumber * recordsPerPage,
                    fileChannel.map( READ_ONLY, pageNumber * recordsPerPage * bytesPerRecord,
                            recordsPerPage * bytesPerRecord ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void release( PersistenceWindow window )
    {
    }

    @Override
    public void flushAll()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public WindowPoolStats getStats()
    {
        return null;
    }

    static class MappedWindow implements PersistenceWindow
    {
        private final int startRecordId;
        private final MappedByteBuffer buffer;
        private final int recordsPerPage;
        private final int recordSize;

        public MappedWindow( int recordsPerPage, int recordSize, int startRecordId, MappedByteBuffer buffer )
        {
            this.recordsPerPage = recordsPerPage;
            this.recordSize = recordSize;
            this.startRecordId = startRecordId;
            this.buffer = buffer;
        }

        @Override
        public Buffer getBuffer()
        {
            return null;
        }

        @Override
        public Buffer getOffsettedBuffer( long id )
        {
            return null;
        }

        @Override
        public int getRecordSize()
        {
            return recordSize;
        }

        @Override
        public long position()
        {
            return startRecordId;
        }

        @Override
        public int size()
        {
            return recordsPerPage;
        }

        @Override
        public void force()
        {
        }

        @Override
        public void close()
        {
        }
    }
}
