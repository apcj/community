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

import static org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore.KEY_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore.TYPE_STORE_BLOCK_SIZE;
import static org.neo4j.kernel.impl.nioneo.store.structure.StoreFileFactory.ConfigurableRecordLength.configurableRecordLength;
import static org.neo4j.kernel.impl.nioneo.store.structure.StoreFileFactory.FixedRecordLength.fixedRecordLength;
import static org.neo4j.kernel.impl.nioneo.store.structure.StoreInitializer.NullStoreInitializer.startsEmpty;

import java.util.Map;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;

public enum StoreFileType
{
    String( "strings", "StringPropertyStore",
            configurableRecordLength( new DynamicStringStore.BlockSizeConfiguration(),
                    IdType.STRING_BLOCK ), startsEmpty() ),

    Array( "arrays", "ArrayPropertyStore",
            configurableRecordLength( new DynamicArrayStore.BlockSizeConfiguration(),
                    IdType.ARRAY_BLOCK ), startsEmpty() ),

    RelationshipTypeName( "names", "StringPropertyStore",
            configurableRecordLength( new DynamicStringStore.FixedBlockSize( TYPE_STORE_BLOCK_SIZE ),
                    IdType.RELATIONSHIP_TYPE_BLOCK), startsEmpty() ),

    PropertyIndexKey( "keys", "StringPropertyStore",
            configurableRecordLength( new DynamicStringStore.FixedBlockSize( KEY_STORE_BLOCK_SIZE ),
                    IdType.PROPERTY_INDEX_BLOCK), startsEmpty() ),

    PropertyIndex( "index", "PropertyIndexStore",
            fixedRecordLength(), startsEmpty(),
            childStores( PropertyIndexKey ) ),

    Property( "propertystore.db", "PropertyStore",
            fixedRecordLength(), startsEmpty(),
            childStores( String, Array, PropertyIndex ) ),

    Node( "nodestore.db", "NodeStore",
            fixedRecordLength(), new NodeStore.CreateReferenceNode() ),

    Relationship( "relationshipstore.db", "NodeStore",
            fixedRecordLength(), startsEmpty() ),

    RelationshipType( "relationshiptypestore.db", "RelationshipTypeStore",
            fixedRecordLength(), startsEmpty(),
            childStores( RelationshipTypeName ) ),

    Neo( "neostore", "NeoStore",
            fixedRecordLength(), new NeoStore.StoreMetadata(),
            childStores( Property, Node, Relationship, RelationshipType ));

    public final String fileNamePart;
    public final String typeDescriptor;
    private StoreFileFactory factory;
    private StoreInitializer initializer;
    private StoreFileType[] childStoreFiles;
    private StoreFileType parentStoreFile;

    StoreFileType( String fileNamePart, String typeDescriptor, StoreFileFactory factory, StoreInitializer initializer, StoreFileType[] childStoreFiles )
    {
        this.fileNamePart = fileNamePart;
        this.typeDescriptor = typeDescriptor;
        this.factory = factory;
        this.initializer = initializer;
        this.childStoreFiles = childStoreFiles;
        for ( StoreFileType childStoreFile : childStoreFiles )
        {
            childStoreFile.parentStoreFile = this;
        }
    }

    StoreFileType( String fileNamePart, String typeDescriptor, StoreFileFactory factory, StoreInitializer initializer )
    {
        this(fileNamePart, typeDescriptor, factory, initializer, childStores());
    }

    private static StoreFileType[] childStores(StoreFileType... childStoreFiles) {
        return childStoreFiles;
    }

    public void createStore( String fileName, Map<?, ?> config )
    {
        for ( StoreFileType childStoreFile : childStoreFiles )
        {
            childStoreFile.createStore(fileName + "." + childStoreFile.fileNamePart, config );
        }

        factory.createStore( fileName, CommonAbstractStore.buildTypeDescriptorAndVersion( typeDescriptor ), config );
        initializer.initialize( fileName, config );
    }

    public String getFileName()
    {
        if (parentStoreFile == null) {
            return fileNamePart;
        }
        return parentStoreFile.getFileName() + "." + fileNamePart;
    }
}