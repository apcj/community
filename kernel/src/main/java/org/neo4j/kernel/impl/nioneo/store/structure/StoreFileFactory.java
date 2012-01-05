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

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.AbstractStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public interface StoreFileFactory
{
    void createStore( String fileName, String typeAndVersionDescriptor, Map<?, ?> config );

    public class FixedRecordLength implements StoreFileFactory
    {
        static FixedRecordLength fixedRecordLength()
        {
            return new FixedRecordLength();
        }

        public void createStore( String fileName, String typeAndVersionDescriptor, Map<?, ?> config )
        {
            IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                    IdGeneratorFactory.class );

            FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );

            AbstractStore.createEmptyStore( fileName, typeAndVersionDescriptor, idGeneratorFactory, fileSystem );
        }
    }

    public class ConfigurableRecordLength implements StoreFileFactory
    {
        static ConfigurableRecordLength configurableRecordLength( RecordLengthConfiguration recordLengthConfiguration, IdType idType )
        {
            return new ConfigurableRecordLength( recordLengthConfiguration, idType );
        }

        private RecordLengthConfiguration recordLengthConfiguration;
        private IdType idType;

        public ConfigurableRecordLength( RecordLengthConfiguration recordLengthConfiguration, IdType idType )
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
}
