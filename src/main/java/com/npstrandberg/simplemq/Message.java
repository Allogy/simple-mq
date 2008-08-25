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
 * A Message is an immutable object returned by calling the recieve methods on a {@link MessageQueue}.
 *
 * @author Niels Peter Strandberg
 * @see MessageQueue#receive()
 */
public interface Message {

    /**
     * Returns the {@link String} body
     *
     * @return the body
     */
    String getBody();

    /**
     * Returns the {@link Serializable} object
     *
     * @return the {@link Serializable} object
     */
    Serializable getObject();

    /**
     * Returns the internal id
     * The internal id is set by the {@link MessageQueue}
     *
     * @return the internal id
     */
    long getId();
}
