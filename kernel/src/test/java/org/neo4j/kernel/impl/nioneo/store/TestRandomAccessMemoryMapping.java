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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.osIsWindows;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.all_stores_total_mapped_memory_size;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.testDirForTest;

import java.io.File;
import java.util.Random;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultLastCommittedTxIdSetter;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.nioneo.store.windowpool.ScanResistantWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestRandomAccessMemoryMapping.MissDrivenRemap.class,
        TestRandomAccessMemoryMapping.ScanResistant.class
})
@Ignore("to be replaced with more fine grained test")
public abstract class TestRandomAccessMemoryMapping
{
    @Test
    public void shouldAchieveHitRationConsistentWithMappedRatioWhenAccessingRecordsRandomly() throws Exception
    {
        // when
        Random random = new Random();
        for ( int i = 0; i < TOTAL_RECORDS; i++ )
        {
            nodeStore.getRecord( random.nextInt( TOTAL_RECORDS ) );
        }

        // then
        assertThat( nodeStore.getWindowPoolStats(), hasHitRatioGreaterThan( 0.4f ) );
    }

    @Test
    public void shouldAchieveHighHitRatioWhenScanningLinearly() throws Exception
    {
        // when
        for ( int i = 0; i < TOTAL_RECORDS; i++ )
        {
            nodeStore.getRecord( i );
        }

        // then
        assertThat( nodeStore.getWindowPoolStats(), hasHitRatioGreaterThan( 0.8f ) );
    }

    @Rule
    public TargetDirectory.TestDirectory store = testDirForTest( TestRandomAccessMemoryMapping.class );

    @RunWith(JUnit4.class)
    public static class MissDrivenRemap extends TestRandomAccessMemoryMapping
    {
        @Override
        WindowPoolFactory windowPoolFactory()
        {
            return new DefaultWindowPoolFactory();
        }
    }

    @RunWith(JUnit4.class)
    public static class ScanResistant extends TestRandomAccessMemoryMapping
    {
        @Override
        WindowPoolFactory windowPoolFactory()
        {
            return new ScanResistantWindowPoolFactory( configTunedToMapAPreciseNumberOfRecords( store.directory() ));
        }
    }

    private static final int MEGA = 1024 * 1024;

    public static final int MAPPED_RECORDS = 1000000;
    public static final int TOTAL_RECORDS = MAPPED_RECORDS * 2;

    private NodeStore nodeStore;

    abstract WindowPoolFactory windowPoolFactory();

    @Before
    public void checkNotRunningOnWindowsBecauseMemoryMappingDoesNotWorkProperlyThere()
    {
        assumeTrue( !osIsWindows() );
    }

    @Before
    public void createStore()
    {
        // given
        File storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
        Config config = configTunedToMapAPreciseNumberOfRecords( storeDir );

        String fileName = new File( storeDir, NeoStore.DEFAULT_NAME + ".nodestore.db" ).getPath();

        createStore( fileName, TOTAL_RECORDS, config );

        nodeStore = new NodeStore( fileName, config, new DefaultIdGeneratorFactory(),
                windowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM );
    }


    private Matcher<WindowPoolStats> hasHitRatioGreaterThan( final float ratio )
    {
        return new TypeSafeMatcher<WindowPoolStats>()
        {
            @Override
            public boolean matchesSafely( WindowPoolStats stats )
            {
                System.out.println( stats );
                return stats.getHitCount() > TOTAL_RECORDS * ratio;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "hitCount > " ).appendValue( TOTAL_RECORDS * ratio );
            }
        };
    }

    private static Config configTunedToMapAPreciseNumberOfRecords( File storeDir )
    {
        return new Config( new ConfigurationDefaults( GraphDatabaseSettings.class ).apply( stringMap(
                    nodestore_mapped_memory_size.name(), mmapSize( MAPPED_RECORDS, NodeStore.RECORD_SIZE ),
                    mapped_memory_page_size.name(), "1K",
                    all_stores_total_mapped_memory_size.name(), mmapSize( MAPPED_RECORDS, NodeStore.RECORD_SIZE ),
                    NodeStore.Configuration.use_memory_mapped_buffers.name(), "true",
                    NodeStore.Configuration.store_dir.name(), storeDir.getPath(),
                    NodeStore.Configuration.neo_store.name(), new File( storeDir, NeoStore.DEFAULT_NAME ).getPath()
            ) ) );
    }

    private void createStore( String fileName, int TOTAL_RECORDS, Config config )
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(),
                new DefaultLastCommittedTxIdSetter(), StringLogger.SYSTEM, new DefaultTxHook() );

        storeFactory.createEmptyStore( fileName, storeFactory.buildTypeDescriptorAndVersion(
                NodeStore.TYPE_DESCRIPTOR ) );

        NodeStore nodeStore = new NodeStore( fileName, config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM );

        for ( int i = 0; i < TOTAL_RECORDS; i++ )
        {
            NodeRecord record = new NodeRecord( nodeStore.nextId(), 0, 0 );
            record.setInUse( true );
            nodeStore.updateRecord( record );
        }
        nodeStore.close();
    }

    private static String mmapSize( int numberOfRecords, int recordSize )
    {
        int bytes = numberOfRecords * recordSize;
        if ( bytes < MEGA )
        {
            throw new IllegalArgumentException( "too few records: " + numberOfRecords );
        }
        return bytes / MEGA + "M";
    }

    @After
    public void closeStore()
    {
        nodeStore.close();
    }
}
