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

import java.util.Comparator;
/**
 * 这两个函数：用于减少像index blocks这样的内部数据结构占用的空间
 */
public interface UserComparator
        extends Comparator<Slice>
{
    String name();

    /**
     * 这个函数的作用就是：如果start < limit，就在[start,limit)中找到一个短字符串，并赋给start返回
     * 简单的comparator实现可能不改变start，这也是正确的
     * @return 一个介于start和limit之间的短字符串
     */
    Slice findShortestSeparator(Slice start, Slice limit);

    /**
     * 这个函数的作用就是：找一个>= key的短字符串，简单的comparator实现可能不改变key，这也是正确的
     * @param key 参考key
     * @return 大于等于key的字符串
     */
    Slice findShortSuccessor(Slice key);
}