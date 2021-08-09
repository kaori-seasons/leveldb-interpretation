/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.complone.base.include;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Status {
    /**
     *  OK status has a null state_.  Otherwise, state_ is a new[] array of the following form:
     *  state_[0..3] == length of message
     *  state_[4]    == code
     *  state_[5..]  == message
     * */
    private byte[] state_;
    private int size_;
    private Code code(){
        if(this.state_ == null) return Code.kOk;
        for(Code code : Code.values()){
            if(code.getCode() == this.state_[4]) return code;
        }
        return Code.kNotFound;
    }

    Status(Code code, Slice msg1, Slice msg2) {

        if(!code.equals(Code.kOk)){
            int len1 = msg1.length();
            int len2 = msg2.length();
            int size = len1 + (len2 > 0 ? (2 + len2) : 0);
            this.size_ = size;
            /**
             * 这里源码是+5，因为在c++中，一个char占1字节，源码增加了':'和' '两个char,
             * if (len2) {
             *     result[5 + len1] = ':';
             *     result[6 + len1] = ' ';
             *     std::memcpy(result + 7 + len1, msg2.data(), len2);
             *   }
             *   java中一个char占两个字节，因此不能直接将char转换为byte，因为':'字符的二进制表示为00111010,
             *   ' '字符的二进制表示为00100000，所以需要在前面补0
             * */
            byte[] result = new byte[size + 7];
            result[0] = (byte)(size >>> 8);
            result[1] = (byte)(size >>> 16);
            result[2] = (byte)(size >>> 24);
            result[3] = (byte)(size >>> 32);
            result[4] = (byte)(code.getCode() & 0xFF);
            System.arraycopy(msg1.getData(), 0, result, 5, len1);
            if(len2 > 0){
                result[5 + len1] = (byte)0;
                result[6 + len1] = (byte)58;
                result[7 + len1] = (byte)0;
                result[8 + len1] = (byte)32;
                System.arraycopy(msg2.getData(), 0, result, len1 +7, len2);
            }
            state_ = result;
        }
    }

    byte[] CopyState(byte[] s) {
        return (byte[]) Arrays.copyOf(s,s.length);
    }

    public Status(){
        super();
        this.state_ = null;
    }
    public Status(Code code, Slice msg){
    }
    public Status(Status rhs){
        rhs.state_ = null;
    }
    /**
     * Return a success status.
     * */
    public static Status OK() { return new Status(); }

    /**
     * Return error status of an appropriate type.
     * */
    public static Status NotFound(final Slice msg) {
        return new Status(Code.kNotFound, msg);
    }
    public static Status Corruption(final Slice msg) {
        return new Status(Code.kCorruption, msg);
    }
    public static Status NotSupported(final Slice msg) {
        return new Status(Code.kNotSupported, msg);
    }
    public static Status InvalidArgument(final Slice msg) {
        return new Status(Code.kInvalidArgument, msg);
    }
    public static Status IOError(final Slice msg) {
        return new Status(Code.kIOError, msg);
    }
    /**
     * Returns true iff the status indicates success.
     * */
    boolean ok() { return (code() == Code.kOk); }

    /**
     * Returns true iff the status indicates a NotFound error.
     * */
    boolean IsNotFound() { return code() == Code.kNotFound; }

    /**
     * Returns true iff the status indicates a Corruption error.
     * */
    boolean IsCorruption() { return code() == Code.kCorruption; }

    /**
     * Returns true iff the status indicates an IOError.
     * */
    boolean IsIOError() { return code() == Code.kIOError; }

    /**
     * Returns true iff the status indicates a NotSupportedError.
     * */
    boolean IsNotSupportedError() { return code() == Code.kNotSupported; }

    /**
     * Returns true iff the status indicates an InvalidArgument.
     * */
    boolean IsInvalidArgument() { return code() == Code.kInvalidArgument; }

     /**
      * Return a string representation of this status suitable for printing.
      * Returns the string "OK" for success.
      * */
    public String toString() {
        if (state_ == null) {
            return "OK";
        }
        String type;
        switch (code()) {
            case kOk:
                type = "OK";
                break;
            case kNotFound:
                type = "NotFound: ";
                break;
            case kCorruption:
                type = "Corruption: ";
                break;
            case kNotSupported:
                type = "Not implemented: ";
                break;
            case kInvalidArgument:
                type = "Invalid argument: ";
                break;
            case kIOError:
                type = "IO error: ";
                break;
            default:
                type = "Unknown code(" + code() + ")";
                break;
        }
        StringBuffer result = new StringBuffer(type);
        String temp = new String(state_, 5, size_, StandardCharsets.UTF_8);
        result.append(temp);
        return result.toString();
    }
}


