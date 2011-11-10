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

import java.util.Map;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import static org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore.TYPE_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore.KEY_STORE_BLOCK_SIZE;

public enum StoreFileType
{
    StringStore( new DynamicStringStore.ConfigurationDrivenBlockSizeCreator() ),
    ArrayStore( new DynamicArrayStore.Creator() ),
    RelationshipTypeNameStore( new DynamicStringStore.FixedBlockSizeCreator( IdType.RELATIONSHIP_TYPE_BLOCK, TYPE_STORE_BLOCK_SIZE ) ),
    PropertyIndexKeyStore( new DynamicStringStore.FixedBlockSizeCreator( IdType.PROPERTY_INDEX_BLOCK, KEY_STORE_BLOCK_SIZE ) ),
    PropertyIndexStore( new PropertyIndexStore.Creator(),
            child( "keys", PropertyIndexKeyStore ) ),
    PropertyStore( new PropertyStore.Creator(),
            child( "strings", StringStore ),
            child( "arrays", ArrayStore ),
            child( "index", PropertyIndexStore ) ),
    NodeStore( new NodeStore.Creator() ),
    RelationshipStore( new RelationshipStore.Creator() ),
    RelationshipTypeStore( new RelationshipTypeStore.Creator(),
            child( "names", RelationshipTypeNameStore ) ),
    NeoStore( new NeoStore.Creator(),
            child( "propertystore.db", PropertyStore ),
            child( "nodestore.db", NodeStore ),
            child( "relationshipstore.db", RelationshipStore ),
            child( "relationshiptypestore.db", RelationshipTypeStore ) );

    private StoreCreator storeCreator;
    private ChildStoreFile[] childStoreFiles;

    StoreFileType( StoreCreator creator, ChildStoreFile... childStoreFiles )
    {
        this.storeCreator = creator;
        this.childStoreFiles = childStoreFiles;
    }

    public void createStore( String fileName, Map<?, ?> config )
    {
        for ( ChildStoreFile childStoreFile : childStoreFiles )
        {
            childStoreFile.storeFileType.createStore(
                    fileName + "." + childStoreFile.fileNamePart, config );
        }

        storeCreator.create( fileName, config );
    }

    static ChildStoreFile child( String fileNamePart, StoreFileType storeFileType )
    {
        return new ChildStoreFile( fileNamePart, storeFileType );
    }

    private static class ChildStoreFile
    {
        private final String fileNamePart;
        private final StoreFileType storeFileType;

        ChildStoreFile( String fileNamePart, StoreFileType storeFileType )
        {
            this.fileNamePart = fileNamePart;
            this.storeFileType = storeFileType;
        }
    }

    public interface StoreCreator
    {
        void create( String fileName, Map<?, ?> config );
    }
}
