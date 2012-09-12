package org.neo4j.kernel.impl.nioneo.store;

import java.nio.channels.FileChannel;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

public class ScanResistantWindowPoolFactory implements WindowPoolFactory
{
    private int targetBytesPerPage = 4096;

    @Override
    public WindowPool create( String storageFileName, int recordSize, FileChannel fileChannel,
                              Config configuration, StringLogger log )
    {
        return new ScanResistantWindowPool( storageFileName, recordSize, targetBytesPerPage, fileChannel );
    }
}
