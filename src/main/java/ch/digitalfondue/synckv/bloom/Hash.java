package ch.digitalfondue.synckv.bloom;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * This class represents a common API for hashing functions.
 */
abstract class Hash {
    /** Constant to denote {@link MurmurHash}. */
    public static final int MURMUR_HASH  = 1;

    /**
     * Get a singleton instance of hash function of a given type.
     * @param type predefined hash type
     * @return hash function instance, or null if type is invalid
     */
    public static Hash getInstance(int type) {
        switch(type) {
            case MURMUR_HASH:
                return MurmurHash.getInstance();
            default:
                return null;
        }
    }

    /**
     * Calculate a hash using all bytes from the input argument,
     * and a provided seed value.
     * @param bytes input bytes
     * @param initval seed value
     * @return hash value
     */
    public int hash(byte[] bytes, int initval) {
        return hash(bytes, bytes.length, initval);
    }

    /**
     * Calculate a hash using bytes from 0 to <code>length</code>, and
     * the provided seed value
     * @param bytes input bytes
     * @param length length of the valid bytes to consider
     * @param initval seed value
     * @return hash value
     */
    public abstract int hash(byte[] bytes, int length, int initval);
}
