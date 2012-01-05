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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.structure.StoreFileType;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.util.FileUtils;

public class StoreUpgrader
{
    private Map<?, ?> originalConfig;
    private UpgradeConfiguration upgradeConfiguration;
    private UpgradableDatabase upgradableDatabase;
    private StoreMigrator storeMigrator;
    private DatabaseFiles databaseFiles;

    public StoreUpgrader( Map<?, ?> originalConfig, UpgradeConfiguration upgradeConfiguration, UpgradableDatabase upgradableDatabase, StoreMigrator storeMigrator, DatabaseFiles databaseFiles )
    {
        this.originalConfig = originalConfig;
        this.upgradeConfiguration = upgradeConfiguration;
        this.upgradableDatabase = upgradableDatabase;
        this.storeMigrator = storeMigrator;
        this.databaseFiles = databaseFiles;
    }

    public void attemptUpgrade( String storageFileName )
    {
        upgradeConfiguration.checkConfigurationAllowsAutomaticUpgrade();
        upgradableDatabase.checkUpgradeable( new File( storageFileName ) );

        File workingDirectory = new File( storageFileName ).getParentFile();
        File upgradeDirectory = new File( workingDirectory, "upgrade" );
        File backupDirectory = new File( workingDirectory, "upgrade_backup" );

        migrateToIsolatedDirectory( storageFileName, upgradeDirectory );

        databaseFiles.moveToBackupDirectory( workingDirectory, backupDirectory );
        backupMessagesLogLeavingInPlaceForNewDatabaseMessages( workingDirectory, backupDirectory );
        databaseFiles.moveToWorkingDirectory( upgradeDirectory, workingDirectory );
    }

    private void backupMessagesLogLeavingInPlaceForNewDatabaseMessages( File workingDirectory, File backupDirectory )
    {
        try
        {
            FileUtils.copyFile( new File( workingDirectory, "messages.log" ),
                    new File( backupDirectory, "messages.log" ) );
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( e );
        }
    }

    private void migrateToIsolatedDirectory( String storageFileName, File upgradeDirectory )
    {
        if (upgradeDirectory.exists()) {
            try
            {
                FileUtils.deleteRecursively( upgradeDirectory );
            }
            catch ( IOException e )
            {
                throw new UnableToUpgradeException( e );
            }
        }
        upgradeDirectory.mkdir();

        String upgradeFileName = new File( upgradeDirectory, NeoStore.DEFAULT_NAME ).getPath();
        Map<Object, Object> upgradeConfig = new HashMap<Object, Object>( originalConfig );
        upgradeConfig.put( "neo_store", upgradeFileName );

        StoreFileType.Neo.createStore( upgradeFileName, upgradeConfig );
        NeoStore neoStore = new NeoStore( upgradeConfig );
        try
        {
            storeMigrator.migrate( new LegacyStore( storageFileName ), neoStore );
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( e );
        }
        finally
        {
            neoStore.close();
        }
    }

    public static class UnableToUpgradeException extends RuntimeException
    {
        public UnableToUpgradeException( Exception cause )
        {
            super( cause );
        }

        public UnableToUpgradeException( String message )
        {
            super( message );
        }
    }
}
