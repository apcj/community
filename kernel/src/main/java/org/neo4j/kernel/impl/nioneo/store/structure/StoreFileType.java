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

import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.AbstractStore;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;

public enum StoreFileType
{
    String( "strings", "StringPropertyStore",
            new DynamicRecordLength( new DynamicStringStore.BlockSizeConfiguration(), IdType.STRING_BLOCK ) ),
    Array( "arrays", "ArrayPropertyStore",
            new DynamicRecordLength( new DynamicArrayStore.BlockSizeConfiguration(), IdType.ARRAY_BLOCK )),
    RelationshipTypeName( "names", "StringPropertyStore",
            new DynamicRecordLength( new DynamicStringStore.FixedBlockSize( TYPE_STORE_BLOCK_SIZE ),
                    IdType.RELATIONSHIP_TYPE_BLOCK) ),
    PropertyIndexKey( "keys", "StringPropertyStore",
            new DynamicRecordLength( new DynamicStringStore.FixedBlockSize( KEY_STORE_BLOCK_SIZE ),
                    IdType.PROPERTY_INDEX_BLOCK) ),
    PropertyIndex( "index", "PropertyIndexStore", new FixedRecordLength(),
            PropertyIndexKey ),
    Property( "propertystore.db", "PropertyStore", new FixedRecordLength(),
            String,
            Array,
            PropertyIndex ),
    Node( "nodestore.db", "NodeStore", new FixedRecordLength(), new NodeStore.Initializer() ),
    Relationship( "relationshipstore.db", "NodeStore", new FixedRecordLength() ),
    RelationshipType( "relationshiptypestore.db", "RelationshipTypeStore", new FixedRecordLength(),
            RelationshipTypeName ),
    Neo( "neostore", "NeoStore", new FixedRecordLength(),
            new NeoStore.Initializer(),
            Property,
            Node,
            Relationship,
            RelationshipType );

    public final String fileNamePart;
    public final String typeDescriptor;
    private StoreFileFamily family;
    private StoreInitializer initializer;
    private StoreFileType[] childStoreFiles;

    StoreFileType( String fileNamePart, String typeDescriptor, StoreFileFamily family, StoreInitializer initializer, StoreFileType... childStoreFiles )
    {
        this.fileNamePart = fileNamePart;
        this.typeDescriptor = typeDescriptor;
        this.family = family;
        this.initializer = initializer;
        this.childStoreFiles = childStoreFiles;
    }

    StoreFileType( String fileNamePart, String typeDescriptor, StoreFileFamily family, StoreFileType... childStoreFiles )
    {
        this(fileNamePart, typeDescriptor, family, new NullStoreInitializer(), childStoreFiles);
    }

    public void createStore( String fileName, Map<?, ?> config )
    {
        for ( StoreFileType childStoreFile : childStoreFiles )
        {
            childStoreFile.createStore(fileName + "." + childStoreFile.fileNamePart, config );
        }

        family.createStore( fileName, CommonAbstractStore.buildTypeDescriptorAndVersion( typeDescriptor ), config );
        initializer.initialize( fileName, config );
    }

    public interface StoreInitializer
    {
        void initialize( String fileName, Map<?, ?> config );
    }

    public static class FixedRecordLength implements StoreFileFamily
    {
        public void createStore( String fileName, String typeAndVersionDescriptor, Map<?, ?> config )
        {
            IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                    IdGeneratorFactory.class );

            FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );

            AbstractStore.createEmptyStore( fileName, typeAndVersionDescriptor, idGeneratorFactory, fileSystem );
        }
    }

    public static class DynamicRecordLength implements StoreFileFamily
    {
        private RecordLengthConfiguration recordLengthConfiguration;
        private IdType idType;

        public DynamicRecordLength( RecordLengthConfiguration recordLengthConfiguration, IdType idType )
        {
            this.recordLengthConfiguration = recordLengthConfiguration;
            this.idType = idType;
        }

        public void createStore( String fileName, String typeAndVersionDescriptor, Map<?, ?> config )
        {
            IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                    IdGeneratorFactory.class );

            FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );

            AbstractDynamicStore.createEmptyStore( fileName, recordLengthConfiguration.getBlockSize( config ), typeAndVersionDescriptor, idGeneratorFactory, fileSystem, idType );
        }

        public interface RecordLengthConfiguration
        {
            public int getBlockSize( Map<?, ?> config );
        }
    }

    interface StoreFileFamily {

        void createStore( String fileName, String typeAndVersionDescriptor, Map<?, ?> config );
    }

    private static class NullStoreInitializer implements StoreInitializer
    {
        public void initialize( String fileName, Map<?, ?> config )
        {
        }
    }
}