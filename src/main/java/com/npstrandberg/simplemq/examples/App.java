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


public class App {

    public static void main(String[] args) {

        MessageQueue queue = MessageQueueService.getMessageQueue("test");

        boolean recieved = queue.send(new MessageInput("hej med dig"));
        System.out.println("MessageImpl recieved by queue? " + recieved);

        {
            Message m = queue.receive();
            System.out.println("MessageImpl: " + m);
            //queue.delete(m);
        }

        {
            Message m = queue.receive();
            System.out.println("MessageImpl: " + m);
            //queue.delete(m);
        }

        try {
            Thread.sleep(1000 * 20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // qs.shutdown();
    }
}
