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
package org.neo4j.kernel.impl.nioneo.store.structure;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.IsCollectionContaining.hasItem;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

public class StoreFileHierarchyTest
{
    @Test
    public void createsProperFiles() throws IOException
    {
        File store = TargetDirectory.forTest( getClass() ).directory( "store", true );
        StoreFileType.NeoStore.createFiles( new File( store, "neostore" ) );

        assertThat( Arrays.asList(store.list()), hasItem("neostore") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.nodestore.db") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.propertystore.db") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.propertystore.db.arrays") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.propertystore.db.index") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.propertystore.db.index.keys") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.propertystore.db.strings") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.relationshipstore.db") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.relationshiptypestore.db") );
        assertThat( Arrays.asList(store.list()), hasItem("neostore.relationshiptypestore.db.names") );
        assertThat( store.list().length, equalTo( 10 ) );
    }

    @SuppressWarnings({"unchecked"})
    public static HashMap defaultConfig()
    {
        HashMap config = new HashMap();
        config.put( IdGeneratorFactory.class, CommonFactories.defaultIdGeneratorFactory() );
        config.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        return config;
    }

}
