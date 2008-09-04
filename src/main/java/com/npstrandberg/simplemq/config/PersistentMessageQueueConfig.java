/*
 * Copyright 2008 Niels Peter Strandberg.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.npstrandberg.simplemq.config;

/**
 * The PersistentMessageQueueConfig is used to configure a "isPersistent" {@link com.npstrandberg.simplemq.MessageQueue}.
 * For a "in memory" MessageQueue, use {@link MessageQueueConfig}.
 *
 * @author Niels Peter Strandberg
 * @see com.npstrandberg.simplemq.MessageQueueService#getMessageQueue(String, MessageQueueConfig)
 * @see com.npstrandberg.simplemq.config.MessageQueueConfig
 */
public class PersistentMessageQueueConfig extends MessageQueueConfig {

    /**
     * The default value for {@link #databaseDirectory} is "";
     */
    public static final String DEFAULT_DB_DIR = "";

    /**
     * The default value for {@link #databaseWriteDelay} is 250 milliseconds
     */
    public static final int DEFAULT_DB_WRITE_DELAY = 250;

    /**
     * The default value for {@link #cached} is 'true'
     */
    public static final boolean DEFAULT_CACHED = true;

    /**
     * Using 'cached' helps prevent out of memory exception
     * when working with many messages in the queues .
     *
     * @link http://hsqldb.org/doc/guide/ch01.html#N1023C
     */
    private final boolean cached;

    /**
     * The file path for the cache files
     * When using relative paths, these paths will be taken relative to the directory in which
     * the shell command to start the Java Virtual Machine was executed
     */
    private final String databaseDirectory;

    /**
     * Indicates that the changes to the database that have been logged are synched
     * to the file system once every 'databaseWriteDelay' milliseconds.
     *
     * @link http://hsqldb.org/doc/guide/ch04.html#N10D67
     */
    private final int databaseWriteDelay;

    /**
     * Constructs a new PersistentMessageQueueConfig with default values
     */
    public PersistentMessageQueueConfig() {
        super();
        databaseWriteDelay = DEFAULT_DB_WRITE_DELAY;
        databaseDirectory = DEFAULT_DB_DIR;
        cached = DEFAULT_CACHED;
    }

    /**
     * Constructs a new PersistentMessageQueueConfig
     *
     * @param messageReviveTime
     * @param messageRemoveTime
     * @param reviveNonDeletedMessagsThreadDelay
     *
     * @param deleteOldMessagesThreadDelay
     * @param databaseDirectory
     * @param databaseWriteDelay
     * @param cached
     */
    public PersistentMessageQueueConfig(int messageReviveTime, int messageRemoveTime,
                                        int reviveNonDeletedMessagsThreadDelay, int deleteOldMessagesThreadDelay, String databaseDirectory,
                                        int databaseWriteDelay, boolean cached) {
        super(messageReviveTime, messageRemoveTime, reviveNonDeletedMessagsThreadDelay, deleteOldMessagesThreadDelay);
        this.databaseDirectory = databaseDirectory;
        this.databaseWriteDelay = databaseWriteDelay;
        this.cached = cached;
    }


    public int getDatabaseWriteDelay() {
        return databaseWriteDelay;
    }


    public String getDatabaseDirectory() {
        return databaseDirectory;
    }


    public boolean isCached() {
        return cached;
    }
}
