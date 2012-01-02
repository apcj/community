/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.neo4j.kernel.Config.STRING_BLOCK_SIZE;

import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.structure.StoreFileType;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    static final String VERSION = "StringPropertyStore v0.A.0";
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";

    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;

    public DynamicStringStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }
    
    @Override
    public void accept( RecordStore.Processor processor, DynamicRecord record )
    {
        processor.processString( this, record );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    public static class Creator implements StoreFileType.StoreCreator
    {
        private final IdType idType;
        private final int blockSize;
        private static final int NULL_BLOCKSIZE = 0;

        public Creator()
        {
            this.idType = IdType.STRING_BLOCK;
            this.blockSize = NULL_BLOCKSIZE;
        }

        public Creator( IdType idType, int blockSize )
        {
            this.idType = idType;
            this.blockSize = blockSize;
        }

        public void create( String fileName, Map<?, ?> config )
        {
            IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                    IdGeneratorFactory.class );

            FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );

            createEmptyStore( fileName, getBlockSize( config ), VERSION, idGeneratorFactory, fileSystem, idType );
        }

        private int getBlockSize( Map<?, ?> config )
        {
            if ( blockSize == NULL_BLOCKSIZE )
            {
                return parseConfiguredBlockSize( config );
            }

            return blockSize;
        }

        private int parseConfiguredBlockSize( Map<?, ?> config )
        {
            int size = DEFAULT_DATA_BLOCK_SIZE;

            String stringBlockSize = (String) config.get( STRING_BLOCK_SIZE );
            if ( stringBlockSize != null )
            {
                int value = Integer.parseInt( stringBlockSize );
                if ( value > 0 )
                {
                    size = value;
                }
            }

            return size;
        }
    }

    @Override
    public void setHighId( long highId )
    {
        super.setHighId( highId );
    }

    @Override
    public long nextBlockId()
    {
        return super.nextBlockId();
    }

}
