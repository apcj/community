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

import java.io.File;
import java.io.IOException;
import java.util.Map;

public enum StoreFileType
{
    StringStore(),
    ArrayStore(),
    PropertyIndexStore(
            child( "keys", StringStore ) ),
    PropertyStore(
            child( "strings", StringStore ),
            child( "arrays", ArrayStore ),
            child( "index", PropertyIndexStore ) ),
    NodeStore(),
    RelationshipStore(),
    RelationshipTypeStore(
            child( "names", StringStore ) ),
    NeoStore(
            child( "propertystore.db", PropertyStore ),
            child( "nodestore.db", NodeStore ),
            child( "relationshipstore.db", RelationshipStore ),
            child( "relationshiptypestore.db", RelationshipTypeStore ) );

    private ChildStoreFile[] childStoreFiles;

    StoreFileType( ChildStoreFile... childStoreFiles )
    {
        this.childStoreFiles = childStoreFiles;
    }

    public void createStore( File storeFile, Map<?,?> config ) throws IOException
    {
        storeFile.createNewFile();

        for ( ChildStoreFile childStoreFile : childStoreFiles )
        {
            childStoreFile.storeFileType.createStore(
                    new File( storeFile.getPath() + "." + childStoreFile.fileNamePart ), config );
        }
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
}
