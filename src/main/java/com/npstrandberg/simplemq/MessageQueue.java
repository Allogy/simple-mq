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

import java.util.List;

/**
 * A MessageQueue is used to send and recieve {@link Message}.
 *
 * @author Niels Peter Strandberg
 */
public interface MessageQueue {

    /**
     * Add a message to the message queue
     *
     * @param messageInput
     * @return true - if the message was added succesfully
     */
    boolean send(MessageInput messageInput);


    /**
     * Add a list of messages to the message queue
     *
     * @param messageInputs
     * @return true - if the messages was added succesfully
     */
    boolean send(List<MessageInput> messageInputs);


    /**
     * A nonblocking recieve of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message receive();


    /**
     * A nonblocking recieve of the first 'no' messages of the message Queue
     * The
     *
     * @param no
     * @return a List of Messages or an empty List - if there is no messages in the message queue
     */
    List<Message> receive(int no);


    /**
     * A nonblocking peek of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message peek();

    /**
     * A nonblocking peek of the first 'no' messages of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    List<Message> peek(int no);


    /**
     * A nonblocking recieve and immediate deletion of the "top" message of the message Queue
     *
     * @return a Message or null - if there is no messages in the message queue
     */
    Message receiveAndDelete();


    /**
     * A nonblocking recieve and immediate deletion of the "top" message of the message Queue
     *
     * @param no
     * @return a List of Messages or an empty List - if there is no messages in the message queue
     */
    List<Message> receiveAndDelete(int no);


    /**
     * Deletes the Message from the message queue
     *
     * @param message
     * @return true - if the message was deleted succesfully
     */
    boolean delete(Message message);


    /**
     * Deletes all the Messages in the List from the message queue
     *
     * @param messages
     * @return true - if the message was deleted succesfully
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
     * The number of messages in the queue
     *
     * @return number of objects in the queue
     */
    long messageCount();

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
}
