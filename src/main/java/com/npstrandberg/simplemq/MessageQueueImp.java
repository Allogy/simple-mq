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
package com.npstrandberg.simplemq;

import com.npstrandberg.simplemq.config.MessageQueueConfig;
import com.npstrandberg.simplemq.config.PersistentMessageQueueConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class MessageQueueImp implements MessageQueue, Serializable {
	
	private static final long serialVersionUID = 4688999278205140460L;

    private static Logger log = LoggerFactory.getLogger(MessageQueue.class);

    private transient final Lock lock = new ReentrantLock();
    private transient final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private transient Connection conn;

    private final MessageQueueConfig queueConfig;
    private final String queueName;
    private boolean deleted;

    /**
     * The shutdown thread for the queue. We have to unregistrer it
     * when we shutdown the database, or else the GC cannot remove it.
     */
    private transient Thread shutdownThread;

    /**
     * Constructs a message queue instance that is not controled by
     * the {@link MessageQueueService}. Will normaly only be used
     * by a remoting framework. For at better JVM local usage, use the
     * MessageQueueService.
     *
     * @see MessageQueueService
     */
    public MessageQueueImp() {
        this("noname", new MessageQueueConfig());
    }


    /**
     * Constructs a message queue instance. Normaly only use by the {@link MessageQueueService}.
     * For message queues that is JVM local, use the {@link MessageQueueService}.
     *
     * @param queueName - should be unique in the MessageQueueService
     * @see MessageQueueService
     */
    public MessageQueueImp(String queueName) {
        this(queueName, new MessageQueueConfig());
    }


    /**
     * Constructs a message queue instance. Normaly only use by the {@link MessageQueueService}.
     * For message queues that is JVM local, use the {@link MessageQueueService}.
     *
     * @param queueName   - should be unique in the MessageQueueService
     * @param queueConfig - configur the message queue
     * @see MessageQueueService
     */
    public MessageQueueImp(String queueName, MessageQueueConfig queueConfig) {

        this.queueName = queueName;
        this.queueConfig = (MessageQueueConfig) Utils.copy(queueConfig);

        boolean hsqldbapplog  = queueConfig.getHsqldbapplog();

        try {
            Class.forName("org.hsqldb.jdbcDriver").newInstance();

            if (queueConfig instanceof PersistentMessageQueueConfig) {
                PersistentMessageQueueConfig pqc = (PersistentMessageQueueConfig) queueConfig;
                String cacheDirectory = (pqc.getDatabaseDirectory() == null) ? "" : pqc.getDatabaseDirectory();

                conn = DriverManager.getConnection("jdbc:hsqldb:file:" + cacheDirectory + "queues/" + queueName
                        + "/" + queueName + (hsqldbapplog ? ";hsqldb.applog=1" : ""), "sa", "");
            } else {
                conn = DriverManager.getConnection("jdbc:hsqldb:mem:" + queueName + (hsqldbapplog ? ";hsqldb.applog=1" : ""), "sa", "");
            }

            createTableAndIndex();

        } catch (Exception e) {
            throw new RuntimeException("unable to initialize "+queueName+" message queue", e);
        }

        setWriteDelay();

        startQueueMaintainers();

        // 'shutdownhook' is called when the JVM shutsdown.
        // Used here to make sure we shutdown properly.
        shutdownThread = new Thread() {
            public void run() {
                log.info("thread calls shutdown");

                boolean closed = true;

                try {
                    closed = conn.isClosed();
                } catch (SQLException e) {
                    // ignore
                }

                if (!closed) {
                    shutdown();
                }

                log.info("thread stoped. Connection was closed? " + closed);
            }
        };

        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }


    public boolean send(List<MessageInput> messageInputs) {
        boolean success = true;

        for (MessageInput messageInput : messageInputs) {
            if (!send(messageInput)) success = false;
        }

        return success;

    }

    public boolean send(MessageInput messageInput) {
        if (messageInput == null) {
            throw new NullPointerException("The messageInput cannot be 'null'");
        }

        byte[] b = null;
        if (messageInput.getObject() != null) {
            b = Utils.serialize(messageInput.getObject());

            // Check if the byte array  can fit in the 'object' column.
            // the 'object' column is a BIGINT and has the same max size as Integer.MAX_VALUE
            if (b.length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The Object is to large, it can only be " + Integer.MAX_VALUE + " bytes.");
            }
        }

        try {
            PreparedStatement ps;
            if (b != null) {
                ps = conn.prepareStatement("INSERT INTO message (body, time, read, object) VALUES(?, ?, ?, ?)");
                ps.setBinaryStream(4, new ByteArrayInputStream(b), b.length);
            } else {
                ps = conn.prepareStatement("INSERT INTO message (body, time, read) VALUES(?, ?, ?)");
            }
            ps.setString(1, messageInput.getBody());
            ps.setLong(2, System.currentTimeMillis());
            ps.setBoolean(3, false);
            ps.executeUpdate();
            ps.close();

            return true;
        } catch (SQLException e) {
            throw new RuntimeException("unable to save message", e);
        }
    }


    public Message receiveAndDelete() {
        List<Message> messages = this.receiveInternal(1, true);

        if (messages.size() > 0) {
            return messages.get(0);
        } else {
            return null;
        }
    }


    public List<Message> receiveAndDelete(int limit) {
        return receiveInternal(limit, true);
    }


    public Message receive() {
        List<Message> messages = this.receiveInternal(1, false);

        if (messages.size() > 0) {
            return messages.get(0);
        } else {
            return null;
        }
    }


    public List<Message> receive(int limit) {
        return receiveInternal(limit, false);
    }


    public Message peek() {
        List<Message> messages = this.peekInternal(1);

        if (messages.size() > 0) {
            return messages.get(0);
        } else {
            return null;
        }
    }


    public List<Message> peek(int limit) {
        return peekInternal(limit);
    }


    private List<Message> peekInternal(int limit) {
        if (limit < 1) limit = 1;

        List<Message> messages = new ArrayList<Message>(limit);

        try {

            // 'ORDER BY time' depends on that the host computer times is always right.
            // 'ORDER BY id' what happens with the 'id' when we hit Long.MAX_VALUE?
            PreparedStatement ps = conn.prepareStatement("SELECT LIMIT 0 " + limit
                    + " id, object, body FROM message WHERE read=false ORDER BY id");

            // The lock is making sure, that a SELECT and DELETE/UPDATE is only
            // done by one thread at a time.
            lock.lock();

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                long id = rs.getLong(1);
                InputStream is = rs.getBinaryStream(2);
                String body = rs.getString(3);

                MessageWrapper mw = new MessageWrapper();
                mw.id = id;
                mw.body = body;
                if (is != null) mw.object = Utils.deserialize(is);

                messages.add(mw);
            }

            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return messages;
    }

    private List<Message> receiveInternal(int limit, boolean delete) {
        if (limit < 1) limit = 1;

        List<Message> messages = new ArrayList<Message>(limit);

        try {

            // 'ORDER BY time' depends on that the host computer times is always right.
            // 'ORDER BY id' what happens with the 'id' when we hit Long.MAX_VALUE?
            PreparedStatement ps = conn.prepareStatement("SELECT LIMIT 0 " + limit
                    + " id, object, body FROM message WHERE read=false ORDER BY id");

            // The lock is making sure, that a SELECT and DELETE/UPDATE is only
            // done by one thread at a time.
            lock.lock();

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                long id = rs.getLong(1);
                InputStream is = rs.getBinaryStream(2);
                String body = rs.getString(3);

                if (delete) {
                    PreparedStatement updateInventory = conn.prepareStatement("DELETE FROM message WHERE id=?");
                    updateInventory.setLong(1, id);
                    updateInventory.executeUpdate();
                } else {
                    PreparedStatement updateInventory = conn.prepareStatement("UPDATE message SET read=? WHERE id=?");
                    updateInventory.setBoolean(1, true);
                    updateInventory.setLong(2, id);
                    updateInventory.executeUpdate();
                }

                MessageWrapper mw = new MessageWrapper();
                mw.id = id;
                mw.body = body;
                if (is != null) mw.object = Utils.deserialize(is);

                messages.add(mw);
            }

            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return messages;
    }

    public boolean delete(List<Message> messages) {

        try {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            for (Message message : messages) {
                if (!(message instanceof MessageWrapper)) {
                    throw new IllegalArgumentException("This instance of 'Message' is not valid.");
                }

                stmt.addBatch("DELETE FROM message WHERE id=" + message.getId());
            }

            int[] updateCounts = stmt.executeBatch();

            // Have we deleted them all
            if (updateCounts.length == messages.size()) {
                return true;
            } else {
                log.error("Not all Messages was deleted! Only {} out of {} were deleted!", updateCounts.length, messages.size());
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                log.error("unable to restore autocommit", e);
            }
        }

        return false;
    }

    public boolean delete(Message message) {

        // The Message instances this queue returns is
        // and instance of 'MessageWrapper', so we only
        // accept them.
        if (!(message instanceof MessageWrapper)) {
            throw new IllegalArgumentException("This instance of 'Message' is not valid.");
        }

        try {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM message WHERE id=?");
            ps.setLong(1, message.getId());
            int i = ps.executeUpdate();
            ps.close();

            return (i > 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void deleteQueue() {
        deleted = true;
        shutdown();

        // if we have a persistent queue, delete the files.
        if (queueConfig instanceof PersistentMessageQueueConfig) {
            PersistentMessageQueueConfig pqc = (PersistentMessageQueueConfig) queueConfig;
            String cacheDirectory = (pqc.getDatabaseDirectory() == null) ? "" : pqc.getDatabaseDirectory();
            Utils.deleteDirectory(new File(cacheDirectory + "queues/", queueName));
        }
    }


    public MessageQueueConfig getMessageQueueConfig() {
        return queueConfig;
    }


    public boolean deleted() {
        return deleted;
    }


    public long messageCount() {
        long count = -1;

        try {
            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(id) FROM message WHERE read=?");
            ps.setBoolean(1, false);

            ResultSet rs = ps.executeQuery();

            rs.next();
            count = rs.getLong(1);
            ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return count;
    }

    public boolean isPersistent() {
        return (queueConfig instanceof PersistentMessageQueueConfig);
    }


    private void createTableAndIndex() throws SQLException {

        // The table may allready exsists if the queue is persistent.
        if (tableExists("message")) return;

        String cached = "";

        if (queueConfig instanceof PersistentMessageQueueConfig) {
            PersistentMessageQueueConfig pqc = (PersistentMessageQueueConfig) queueConfig;

            if (pqc.isCached()) {
                cached = "CACHED ";
            }
        }

        Statement st = conn.createStatement();

        st.execute("CREATE " + cached + "TABLE message (id BIGINT IDENTITY PRIMARY KEY, object LONGVARBINARY, body VARCHAR, time BIGINT, read BOOLEAN)");
        st.execute("CREATE INDEX id_index ON message(id)");
        st.execute("CREATE INDEX time_index ON message(time)");
        st.execute("CREATE INDEX read_index ON message(read)");
        st.close();
    }

    // To check if a 'tableName' table allready exsists in the db.
    private boolean tableExists(String tableName) throws SQLException {

        PreparedStatement stmt = null;
        ResultSet results = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
            results = stmt.executeQuery();
            return true;  // if table does exist, no rows will ever be returned
        }
        catch (SQLException e) {
            return false;  // if table does not exist, an exception will be thrown
        }
        finally {
            if (results != null) {
                results.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }


    private void setWriteDelay() {
        if (queueConfig instanceof PersistentMessageQueueConfig) {
            PersistentMessageQueueConfig pqc = (PersistentMessageQueueConfig) queueConfig;

            try {
                Statement st = conn.createStatement();

                st.execute("SET WRITE_DELAY " + pqc.getDatabaseWriteDelay() + " MILLIS");
                st.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // A 'SHUTDOWN' command is send to the db, so that
    // it can flush changes and cleanup before the JVM halts.
    private void shutdown() {
        try {
            Statement st = conn.createStatement();
            st.execute("SHUTDOWN");
            st.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // shutdown schedular
        shutdownAndAwaitTermination(scheduler);

        // unreg shutdown thread
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
    }

    // 'QueueMaintainers' are 2 background threads. 
    // One deletes 'to old' messages, and the other
    // 'revives' messages that has been read, but not deleted.
    private void startQueueMaintainers() {

        // delete 'to old' messages
        final Runnable deleteToOldMessagesRunnable = new Runnable() {
            public void run() {
                log.info("Delete 'to old' messages: ");

                try {
                    PreparedStatement ps = conn.prepareStatement("SELECT id FROM message WHERE time<?");

                    ps.setLong(1, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(queueConfig.getMessageRemoveTime()));

                    ResultSet rs = ps.executeQuery();
                    List<Long> ids = new ArrayList<Long>();

                    while (rs.next()) {
                        ids.add(rs.getLong(1));
                    }

                    ps.close();
                    ps = conn.prepareStatement("DELETE FROM message WHERE id=?");

                    for (Long id : ids) {
                        ps.setLong(1, id);
                        ps.executeUpdate();
                    }

                    ps.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };


        scheduler.scheduleWithFixedDelay(deleteToOldMessagesRunnable,
                queueConfig.getDeleteOldMessagesThreadDelay(),
                queueConfig.getDeleteOldMessagesThreadDelay(), TimeUnit.SECONDS);

        // revive messages that has been read, but not deleted.
        final Runnable reviveRunnable = new Runnable() {
            public void run() {
                log.debug("reviving stale messages");

                //!!!: This operation is technically unsafe, couldn't it be optimized to a single update command? would that make an ever-growing transaction log file?
                try {
                    PreparedStatement ps = conn.prepareStatement("SELECT id FROM message WHERE time<? AND read=true");

                    ps.setLong(1, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(queueConfig.getMessageReviveTime()));

                    ResultSet rs = ps.executeQuery();
                    List<Long> ids = new ArrayList<Long>();

                    while (rs.next()) {
                        ids.add(rs.getLong(1));
                    }

                    ps.close();

                  if (ids.isEmpty()) {
                      log.debug("no {} messages", queueName);
                  } else {
                    log.info("{} {} messages have been revived", ids.size(), queueName);
                    ps = conn.prepareStatement("UPDATE message SET read=? WHERE id=?");

                    for (Long id : ids) {
                        ps.setBoolean(1, false);
                        ps.setLong(2, id);
                        ps.executeUpdate();
                    }

                    ps.close();
                  }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        scheduler.scheduleWithFixedDelay(reviveRunnable, queueConfig.getReviveNonDeletedMessagsThreadDelay(),
                queueConfig.getReviveNonDeletedMessagsThreadDelay(), TimeUnit.SECONDS);

    }

    // Shutsdown the 2 Queue Maintainer threads.
    private void shutdownAndAwaitTermination(ExecutorService pool) {

        pool.shutdown();    // Disable new tasks from being submitted

        try {

            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                pool.shutdownNow();    // Cancel currently executing tasks

                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {

            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();

            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }


    // A Message returned by this queue, is an instance of MessageWrapper.
    private class MessageWrapper implements Message, Serializable {
		private static final long serialVersionUID = 7465209569623629016L;
		private String body;
        private long id;
        private Serializable object;


        public String getBody() {
            return body;
        }


        public Serializable getObject() {
            return object;
        }


        public long getId() {
            return id;
        }
    }
}
