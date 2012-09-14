/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.store.windowpool;

import static java.lang.String.format;

import java.io.IOException;
import java.nio.channels.FileChannel;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.paging.Cart;
import org.neo4j.kernel.impl.util.StringLogger;

public class ScanResistantWindowPoolFactory implements WindowPoolFactory
{
    private final int targetBytesPerPage;
    private final Cart cart;

    public ScanResistantWindowPoolFactory( Config configuration )
    {
        this.targetBytesPerPage = pageSize( configuration );
        this.cart = new Cart( mappablePages( configuration, targetBytesPerPage ) );
    }

    private static int pageSize( Config configuration )
    {
        long pageSize = configuration.get( GraphDatabaseSettings.mapped_memory_page_size );
        if ( pageSize > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( format( "configured page size [%d bytes] is too large", pageSize ) );
        }
        return (int) pageSize;
    }

    private static int mappablePages( Config configuration, int targetBytesPerPage )
    {
        long bytes = configuration.get( GraphDatabaseSettings.all_stores_total_mapped_memory_size );
        long pageCount = bytes / targetBytesPerPage;
        if ( pageCount > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( format( "configured page size [%d bytes] and mapped memory [%d bytes]" +
                    " implies too many pages", targetBytesPerPage, bytes ) );
        }
        return (int) pageCount;
    }

    @Override
    public WindowPool create( String storageFileName, int recordSize, FileChannel fileChannel,
                              Config configuration, StringLogger log )
    {
        try
        {
            return new ScanResistantWindowPool( storageFileName, recordSize, targetBytesPerPage,
                    new FileMapper( fileChannel ), cart );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }
}
