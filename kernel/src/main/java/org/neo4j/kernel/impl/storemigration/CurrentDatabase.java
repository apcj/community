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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.store.structure.StoreFileType;

public class CurrentDatabase
{
    private Map<String, String> fileNamesToTypeDescriptors = new HashMap<String, String>();

    public CurrentDatabase()
    {
        fileNamesToTypeDescriptors.put( NeoStore.DEFAULT_NAME, StoreFileType.Neo.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.nodestore.db", StoreFileType.Node.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db", StoreFileType.Property.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.arrays", StoreFileType.Array.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index", StoreFileType.PropertyIndex.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.index.keys", StoreFileType.PropertyIndexKey.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.propertystore.db.strings", StoreFileType.String.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.relationshipstore.db", StoreFileType.Relationship.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db", StoreFileType.RelationshipType.typeDescriptor );
        fileNamesToTypeDescriptors.put( "neostore.relationshiptypestore.db.names", StoreFileType.RelationshipTypeName.typeDescriptor );
    }

    public boolean storeFilesAtCurrentVersion( File storeDirectory )
    {
        for ( String fileName : fileNamesToTypeDescriptors.keySet() )
        {
            String expectedVersion = buildTypeDescriptorAndVersion( fileNamesToTypeDescriptors.get( fileName ) );
            FileChannel fileChannel = null;
            byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
            try
            {
                File storeFile = new File( storeDirectory, fileName );
                if ( !storeFile.exists() )
                {
                    return false;
                }
                fileChannel = new RandomAccessFile( storeFile, "r" ).getChannel();
                fileChannel.position( fileChannel.size() - expectedVersionBytes.length );
                byte[] foundVersionBytes = new byte[expectedVersionBytes.length];
                fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
                if ( !expectedVersion.equals( UTF8.decode( foundVersionBytes ) ) )
                {
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( fileChannel != null )
                {
                    try
                    {
                        fileChannel.close();
                    }
                    catch ( IOException e )
                    {
                        // Ignore exception on close
                    }
                }
            }
        }
        return true;
    }
}
