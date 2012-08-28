package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.AbstractNameRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;

import static org.neo4j.kernel.impl.nioneo.store.AbstractNameStore.NAME_STORE_BLOCK_SIZE;

/**
 * This class lives here instead of somewhere else in order to be able to access the {@link Command} implementations.
 *
 * @author Tobias Lindaaker
 */
public class TransactionWriter
{
    private final LogBuffer buffer;
    private final int localId;

    public TransactionWriter( LogBuffer buffer, int localId )
    {
        this.buffer = buffer;
        this.localId = localId;
    }

    // Transaction coordination

    public void start( int masterId, int myId ) throws IOException
    {
        start( XidImpl.getNewGlobalId(), masterId, myId, System.currentTimeMillis() );
    }

    public void start( byte[] globalId, int masterId, int myId, long startTimestamp ) throws IOException
    {
        Xid xid = new XidImpl( globalId, NeoStoreXaDataSource.BRANCH_ID );
        LogIoUtils.writeStart( buffer, this.localId, xid, masterId, myId, startTimestamp );
    }

    public void prepare() throws IOException
    {
        prepare( System.currentTimeMillis() );
    }

    public void prepare( long prepareTimestamp ) throws IOException
    {
        LogIoUtils.writePrepare( buffer, localId, prepareTimestamp );
    }

    public void commit( boolean twoPhase, long txId ) throws IOException
    {
        commit( twoPhase, txId, System.currentTimeMillis() );
    }

    public void commit( boolean twoPhase, long txId, long commitTimestamp ) throws IOException
    {
        LogIoUtils.writeCommit( twoPhase, buffer, localId, txId, commitTimestamp );
    }

    public void done() throws IOException
    {
        LogIoUtils.writeDone( buffer, localId );
    }

    // Transaction data

    public void propertyKey( int id, String key, int... dynamicIds ) throws IOException
    {
        write( new Command.PropertyIndexCommand( null, withName( new PropertyIndexRecord( id ), dynamicIds, key ) ) );
    }

    public void relationshipType( int id, String label, int... dynamicIds ) throws IOException
    {
        write( new Command.RelationshipTypeCommand( null,
                                                    withName( new RelationshipTypeRecord( id ), dynamicIds, label ) ) );
    }

    public void create( NodeRecord node ) throws IOException
    {
        node.setCreated();
        update( node );
    }

    public void update( NodeRecord node ) throws IOException
    {
        node.setInUse( true );
        add( node );
    }

    public void delete( NodeRecord node ) throws IOException
    {
        node.setInUse( false );
        add( node );
    }

    public void create( RelationshipRecord relationship ) throws IOException
    {
        relationship.setCreated();
        update( relationship );
    }

    public void update( RelationshipRecord relationship ) throws IOException
    {
        relationship.setInUse( true );
        add( relationship );
    }

    public void delete( RelationshipRecord relationship ) throws IOException
    {
        relationship.setInUse( false );
        add( relationship );
    }

    public void create( PropertyRecord property ) throws IOException
    {
        property.setCreated();
        update( property );
    }

    public void update( PropertyRecord property ) throws IOException
    {
        property.setInUse( true );
        add( property );
    }

    public void delete( PropertyRecord property ) throws IOException
    {
        property.setInUse( false );
        add( property );
    }

    // Internals

    private void add( NodeRecord node ) throws IOException
    {
        write( new Command.NodeCommand( null, node ) );
    }

    private void add( RelationshipRecord relationship ) throws IOException
    {
        write( new Command.RelationshipCommand( null, relationship ) );
    }

    private void add( PropertyRecord property ) throws IOException
    {
        write( new Command.PropertyCommand( null, property ) );
    }

    private void write( Command command ) throws IOException
    {
        LogIoUtils.writeCommand( buffer, localId, command );
    }

    private static <T extends AbstractNameRecord> T withName( T record, int[] dynamicIds, String name )
    {
        if ( dynamicIds == null || dynamicIds.length == 0 )
        {
            throw new IllegalArgumentException( "No dynamic records for storing the name." );
        }
        record.setInUse( true );
        byte[] data = PropertyStore.encodeString( name );
        if ( data.length > dynamicIds.length * NAME_STORE_BLOCK_SIZE )
        {
            throw new IllegalArgumentException(
                    String.format( "[%s] is too long to fit in %d blocks", name, dynamicIds.length ) );
        }
        else if ( data.length <= (dynamicIds.length - 1) * NAME_STORE_BLOCK_SIZE )
        {
            throw new IllegalArgumentException(
                    String.format( "[%s] is to short to fill %d blocks", name, dynamicIds.length ) );
        }

        for ( int i = 0; i < dynamicIds.length; i++ )
        {
            byte[] part = new byte[Math.min( NAME_STORE_BLOCK_SIZE, data.length - i * NAME_STORE_BLOCK_SIZE )];
            System.arraycopy( data, i * NAME_STORE_BLOCK_SIZE, part, 0, part.length );

            DynamicRecord dynamicRecord = new DynamicRecord( dynamicIds[i] );
            dynamicRecord.setInUse( true );
            dynamicRecord.setData( part );
            dynamicRecord.setCreated();
            record.addNameRecord( dynamicRecord );
        }
        record.setNameId( dynamicIds[0] );
        return record;
    }
}
