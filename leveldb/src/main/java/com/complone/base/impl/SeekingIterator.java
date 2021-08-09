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
package com.complone.base.impl;

import com.google.common.collect.PeekingIterator;

import java.util.Map;

public interface SeekingIterator<K, V> extends PeekingIterator<Map.Entry<K, V>>
{
    /**
     * 将迭代器重置到block的起始位置
     */
    void seekToFirst();

    /**
     * 下一个元素的key大于等于当前指定的key
     */
    void seek(K targetKey);
}