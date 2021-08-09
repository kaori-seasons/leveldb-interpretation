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
package com.complone.base.table;

import com.complone.base.include.Slice;

public class BytewiseComparator implements UserComparator
{
    @Override
    public String name()
    {
        return "leveldb.BytewiseComparator";
    }

    @Override
    public int compare(Slice sliceA, Slice sliceB)
    {
        return sliceA.compareTo(sliceB);
    }

    /**
     * 如果start < limit，就在[start,limit)中找到一个短字符串，并赋给start返回
     * @param start 起始的字符串
     * @param limit 结束的字符串
     * @return 返回的短字符串
     */
    @Override
    public Slice findShortestSeparator(
            Slice start,
            Slice limit)
    {
        // 计算公共前缀的长度
        int sharedBytes = BlockBuilder.calculateSharedBytes(start, limit);

        // 如果一个字符串是另一个的前缀，则不需要压缩
        if (sharedBytes < Math.min(start.length(), limit.length())) {

            int lastSharedByte = start.getUnsignedByte(sharedBytes);
            // 尝试执行字符start[diff_index]++，设置start长度为diff_index+1，并返回
            // ++条件：字符< oxff 并且字符+1 < limit上该index的字符
            if (lastSharedByte < 0xff && lastSharedByte + 1 < limit.getUnsignedByte(sharedBytes)) {
                Slice result = start.copySlice(0, sharedBytes + 1);
                result.setByte(sharedBytes, lastSharedByte + 1);

                assert (compare(result, limit) < 0) : "start must be less than last limit";
                return result;
            }
        }
        return start;
    }

    /**
     * 找一个>= key的短字符串,
     * @param key 参考key
     * @return >=key的短字符串
     */

    @Override
    public Slice findShortSuccessor(Slice key)
    {
        // 返回一个大于key短字符串
        // 找到第一个可以++的字符，执行++后，截断字符串；
        // 如果找不到说明*key的字符都是0xff啊，那就不作修改，直接返回

        for (int i = 0; i < key.length(); i++) {
            int b = key.getUnsignedByte(i);
            if (b != 0xff) {
                Slice result = key.copySlice(0, i + 1);
                result.setByte(i, b + 1);
                return result;
            }
        }
        // key已经是他这个长度的最大key了
        return key;
    }
}
