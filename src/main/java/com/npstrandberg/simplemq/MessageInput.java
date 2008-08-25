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


import java.io.Serializable;

/**
 * An instance of this class is used to add message data to a {@link MessageQueue}.
 *
 * @author Niels Peter Strandberg
 */
public class MessageInput implements Serializable {

    private String body;
    private Serializable object;

    /**
     * Constructs a new MessageInput
     */
    public MessageInput() {
    }

    /**
     * Constructs a new MessageInput and add the body
     *
     * @param body
     */
    public MessageInput(String body) {
        this.body = body;
    }


    public String getBody() {
        return body;
    }


    public Serializable getObject() {
        return object;
    }

    public void setObject(Serializable object) {
        this.object = object;
    }

}
