package com.npstrandberg.simplemq;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class TestInMemoryQueue {

    private MessageQueue queue;
    private static final String TEST_DATABASE = "test-database";

    @Before
    public void setUp() {
        queue = MessageQueueService.getMessageQueue(TEST_DATABASE);
        assertFalse(queue.deleted());
    }

    @Test
    public void testQueueService() {
        Collection<String> queues = MessageQueueService.getMessageQueueNames();
        assertTrue(queues.contains(TEST_DATABASE));
    }

    @Test
    public void testMessageQueue() {
        assertNotNull(queue);
        assertTrue(queue instanceof Serializable);
    }

    @Test
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
            Message msg = queue.receiveAndDelete();
            assertTrue(msg instanceof Serializable);
            assertEquals(msg.getBody(), "hello");
            assertEquals(1, queue.messageCount());
        }
        {
            Message msg = queue.receive();
            assertEquals(msg.getObject(), "there");
            queue.delete(msg);
            assertEquals(0, queue.messageCount());
        }
        {
            Message msg = queue.receive();
            assertNull(msg);
        }

        {
            Message msg = queue.receiveAndDelete();
            assertNull(msg);
        }

    }


    @Test(expected = java.lang.NullPointerException.class)
    public void sendNullMessageInput() throws NullPointerException {
        queue.send((MessageInput) null);
    }


    @Test
    public void testSendListOfMessages() {

        List<MessageInput> list = new ArrayList<MessageInput>();
        list.add(new MessageInput("hello"));
        list.add(new MessageInput("hello2"));

        queue.send(list);

        assertEquals(2, queue.messageCount());

        List<Message> messages = queue.receiveAndDelete(2);

        assertEquals(0, queue.messageCount());

    }

    @Test
    public void testDeleteListOfMessages() {

        List<MessageInput> list = new ArrayList<MessageInput>();
        list.add(new MessageInput("hello"));
        list.add(new MessageInput("hello2"));

        assertTrue(queue.send(list));

        assertEquals(2, queue.messageCount());

        List<Message> messages = queue.receive(2);

        assertTrue(queue.delete(messages));

    }


    @After
    public void tearDown() {
        assertFalse(queue.deleted());
        MessageQueueService.deleteMessageQueue(TEST_DATABASE);
        assertTrue(queue.deleted());

        Collection<String> queues = MessageQueueService.getMessageQueueNames();
        assertFalse(queues.contains(TEST_DATABASE));
    }

}
