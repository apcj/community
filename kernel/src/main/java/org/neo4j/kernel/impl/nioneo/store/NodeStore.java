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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.structure.StoreFileType;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractStore implements Store
{
    public static final String TYPE_DESCRIPTOR = "NodeStore";

    // in_use(byte)+next_rel_id(int)+next_prop_id(int)
    public static final int RECORD_SIZE = 9;

    public NodeStore( String fileName, Map<?,?> config )
    {
        super( fileName, config, IdType.NODE );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    public static class Creator implements StoreFileType.StoreCreator {

        public void create( String fileName, Map<?, ?> config )
        {
            IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                    IdGeneratorFactory.class );
            createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory );
            NodeStore store = new NodeStore( fileName, config );
            NodeRecord nodeRecord = new NodeRecord( store.nextId() );
            nodeRecord.setInUse( true );
            store.updateRecord( nodeRecord );
            store.close();
        }
    }

    public NodeRecord getRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            NodeRecord record = getRecord( id, window, false );
            return record;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public void updateRecord( NodeRecord record, boolean recovered )
    {
        assert recovered;
        setRecovered();
        try
        {
            updateRecord( record );
            registerIdFromUpdateRecord( record.getId() );
        }
        finally
        {
            unsetRecovered();
        }
    }

    public void updateRecord( NodeRecord record )
    {
        PersistenceWindow window = acquireWindow( record.getId(),
            OperationType.WRITE );
        try
        {
            updateRecord( record, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public boolean loadLightNode( long id )
    {
        PersistenceWindow window = null;
        try
        {
            window = acquireWindow( id, OperationType.READ );
        }
        catch ( InvalidRecordException e )
        {
            // ok id to high
            return false;
        }

        try
        {
            NodeRecord record = getRecord( id, window, true );
            if ( record == null )
            {
                return false;
            }
            return true;
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private NodeRecord getRecord( long id, PersistenceWindow window,
        boolean check )
    {
        Buffer buffer = window.getOffsettedBuffer( id );

        // [    ,   x] in use bit
        // [    ,xxx ] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            if ( check )
            {
                return null;
            }
            throw new InvalidRecordException( "Record[" + id + "] not in use" );
        }

        long nextRel = buffer.getUnsignedInt();
        long nextProp = buffer.getUnsignedInt();

        long relModifier = (inUseByte & 0xEL) << 31;
        long propModifier = (inUseByte & 0xF0L) << 28;

        NodeRecord nodeRecord = new NodeRecord( id );
        nodeRecord.setInUse( inUse );
        nodeRecord.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
        nodeRecord.setNextProp( longFromIntAndMod( nextProp, propModifier ) );
        return nodeRecord;
    }

    private void updateRecord( NodeRecord record, PersistenceWindow window )
    {
        long id = record.getId();
        Buffer buffer = window.getOffsettedBuffer( id );
        if ( record.inUse() )
        {
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();

            short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((nextRel & 0x700000000L) >> 31);
            short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short)((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = (short)((Record.IN_USE.byteValue() | relModifier | propModifier));
            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) nextRel ).putInt( (int) nextProp );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
            if ( !isInRecoveryMode() )
            {
                freeId( id );
            }
        }
    }

    @Override
    public String toString()
    {
        return "NodeStore";
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.add( getWindowPoolStats() );
        return list;
    }

    @Override
    public void logIdUsage( StringLogger logger )
    {
        NeoStore.logIdUsage( logger, this );
    }
}