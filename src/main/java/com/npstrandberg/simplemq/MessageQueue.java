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

import java.util.Collection;
import java.util.List;

/**
 * A MessageQueue is used to send and recieve {@link Message}.
 *
 * @author Niels Peter Strandberg
 */
public interface MessageQueue
{

    String getQueueName();

    /**
     * Add a message to the message queue
     *
     * @param messageInput commonly of type MessageInput.
     * @see MessageInput
     */
    void send(Message messageInput);


    /**
     * Best effort attempt to send all the provided messages. Problematic for lack of atomicity, if an exception is thrown,
     * some messages may have already been sent.
     *
     * @param messageInputs a collection of messages, commonly of type MessageInput
     */
    void send(Collection<Message> messageInputs);


    /**
     * A nonblocking recieve of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message receive();


    /**
     * A nonblocking recieve of the first 'n' messages of the message Queue
     * The
     *
     * @param n
     * @return a List of Messages or an empty List - if there is no messages in the message queue
     */
    List<Message> receive(int n);


    /**
     * A nonblocking peek of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message peek();

    /**
     * Returns the one message (if any) that has the provided duplicate suppression key.
     *
     * @param dupeSuppressionKey
     * @return
     */
    Message peek(String dupeSuppressionKey);

    /**
     * A nonblocking peek of the first 'n' messages of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    List<Message> peek(int n);


    /**
     * A nonblocking recieve and immediate deletion of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message receiveAndDelete();


    /**
     * A nonblocking recieve and immediate deletion of the "top" message of the message Queue
     *
     * @param n
     * @return a List of Messages or an empty List - if there is no messages in the message queue
     */
    List<Message> receiveAndDelete(int n);

    /**
     * Deletes the Message from the message queue
     *
     * @param message
     * @return true - if the message was deleted, false if it did not exist
     */
    boolean delete(Message message);


    /**
     * Deletes all the Messages in the List from the message queue
     *
     * @param messages
     * @return true - if any of the messages were deleted
     */
    boolean delete(List<Message> messages);

    /**
     * Returns the copy/clone of the MessageQueueConfig used to create this message queue.
     * This is the only way to change this queues config at runtime
     *
     * @return this message queues MessageQueueConfig
     */
    MessageQueueConfig getMessageQueueConfig();

    /**
     * Is this message queue deleted
     *
     * @return true - if this message queue was deleted successfully
     */
    boolean deleted();

    /**
     * The number of messages in the queue. May or may not include unread or active messages.
     *
     * @return
     */
    @Deprecated
    long messageCount();

    /**
     * The number of messages in the queue that have not been seen.
     * @return
     */
    int unreadMessageCount();

    /**
     * The number of messages in the queue including those that are being worked (have been read) and those that are stale before they have been revived.
     * @return
     */
    int totalMessageCount();

    /**
     * Is this message queue is persistent
     *
     * @return true - if this message queue is persistent
     */
    boolean isPersistent();

    /**
     * Delete this message queue and all messages it may contain
     */
    void deleteQueue();

    /**
     * Suspends all message queue operations & releases allocated resources.
     */
    void shutdown();

    /**
     * Shutdown the queue (preventing messages from coming in or out), call
     * the provided messageVisitor on the final list of all messages (including
     * those that might be presently worked on, or stale before being revived).
     *
     * If the messageVisitor does not throw an exception, then the queue itself
     * is deleted (e.g. if it was a persistant queue).
     */
    void decommissionQueue(MessageVisitor messageVisitor);

}
