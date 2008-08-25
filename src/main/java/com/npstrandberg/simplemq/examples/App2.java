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
package com.npstrandberg.simplemq.examples;

import com.npstrandberg.simplemq.Message;
import com.npstrandberg.simplemq.MessageInput;
import com.npstrandberg.simplemq.MessageQueue;
import com.npstrandberg.simplemq.MessageQueueService;
import com.npstrandberg.simplemq.config.PersistentMessageQueueConfig;
import static com.npstrandberg.simplemq.config.PersistentMessageQueueConfig.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class App2 {

    private static Log logger = LogFactory.getLog(App2.class);

    private static final String DB = "test";


    public static void main(String... args) {

        PersistentMessageQueueConfig config = new PersistentMessageQueueConfig(
                DEFAULT_MSG_REVIVE_TIME,
                DEFAULT_MSG_REMOVE_TIME,
                DEFAULT_REVIVE_THREAD_DELAY,
                DEFAULT_DELETE_THREAD_DELAY,
                DEFAULT_DB_DIR,
                DEFAULT_DB_WRITE_DELAY,
                DEFAULT_CACHED
        );

        MessageQueue queue = MessageQueueService.getMessageQueue(DB, config);

        logger.info("Persistent: " + queue.isPersistent());

        for (int i = 0; i < 100; i++) {
            MessageInput mi = new MessageInput();
            mi.setObject("count: " + i);
            queue.send(mi);
        }

        // Try to retrieve (dequeue) the message, and then delete it.
        Message msg = null;
        int count = 0;
        while (count < 5) {
            msg = queue.receive();

            if (msg == null) {
                logger.info("nothing... retrying");
                try {
                    Thread.sleep(3000);
                    count++;
                } catch (Exception ex) {
                }
                continue;
            }

            String text = msg.getBody();
            String body = (String) msg.getObject();
            logger.info("msg : " + text + " " + body);
            queue.delete(msg);
            logger.info("Deleted message id " + msg.getId());

        }

        logger.info("Deleted queue!");
        MessageQueueService.deleteMessageQueue(DB);
        logger.info("Done!");
    }
}
