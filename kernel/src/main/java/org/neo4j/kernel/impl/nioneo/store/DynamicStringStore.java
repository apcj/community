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

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.structure.StoreFileType;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;

    public DynamicStringStore( StoreFileType storeFileType, String fileName, Map<?,?> config, IdType idType )
    {
        super( storeFileType, fileName, config, idType );
    }
    
    @Override
    public void accept( RecordStore.Processor processor, DynamicRecord record )
    {
        processor.processString( this, record );
    }

    public static class BlockSizeConfiguration implements StoreFileType.DynamicRecordLength.RecordLengthConfiguration {

        public int getBlockSize( Map<?, ?> config )
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

    public static class FixedBlockSize implements StoreFileType.DynamicRecordLength.RecordLengthConfiguration {

        private final int blockSize;

        public FixedBlockSize( int blockSize )
        {
            this.blockSize = blockSize;
        }

        public int getBlockSize( Map<?, ?> config )
        {
            return blockSize;
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
