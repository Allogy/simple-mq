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

import junit.framework.TestCase;

import java.util.Collection;


public class TestPersistentMessageQueue extends TestCase {

    private MessageQueue queue;
    private static final String TEST_DATABASE = "test-database";

    @Override
    public void setUp() {
        queue = MessageQueueService.getMessageQueue(TEST_DATABASE, true);
        assertFalse(queue.deleted());
    }

    public void testQueueService() {
        Collection<String> queues = MessageQueueService.getMessageQueueNames();
        assertTrue(queues.contains(TEST_DATABASE));
    }

    public void testMessageQueue() {
        assertNotNull(queue);
    }

    public void testAddAndRecieve() {

        queue.send(new MessageInput("hello"));

        MessageInput mi = new MessageInput();
        mi.setObject("there");
        queue.send(mi);

        assertEquals(2, queue.messageCount());

        // test that I get the same queue instance back
        queue = MessageQueueService.getMessageQueue(TEST_DATABASE);
        assertEquals(2, queue.messageCount());

        {
            Message msg = queue.receive();
            assertEquals(msg.getBody(), "hello");
            queue.delete(msg);
            assertEquals(1, queue.messageCount());
        }
        {
            Message msg = queue.receive();
            assertEquals((String) msg.getObject(), "there");
            queue.delete(msg);
            assertEquals(0, queue.messageCount());
        }
        {
            Message msg = queue.receive();
            assertNull(msg);
        }

    }


    @Override
    public void tearDown() {
        assertFalse(queue.deleted());
        MessageQueueService.deleteMessageQueue(TEST_DATABASE);
        assertTrue(queue.deleted());

        Collection<String> queues = MessageQueueService.getMessageQueueNames();
        assertFalse(queues.contains(TEST_DATABASE));
    }

}
