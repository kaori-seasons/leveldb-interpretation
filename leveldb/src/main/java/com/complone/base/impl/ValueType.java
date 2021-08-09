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

/**
 * leveldb中，值的类型只有两种，一种是有效数据，一种是删除数据。
 * 因为值类型主要和对象键配合使用，这样就可以知道该对象是有值的还是被删除的。
 * 在leveldb中更新和删除都不会直接修改数据，而是新增一条记录，后期合并会删除老旧数据。
 */
public enum ValueType {
    DELETION(0x00),
    VALUE(0x01);

    public static ValueType getValueTypeByPersistentId(int persistentId)
    {
        switch (persistentId) {
            case 0:
                return DELETION;
            case 1:
                return VALUE;
            default:
                throw new IllegalArgumentException("Unknown persistentId " + persistentId);
        }
    }

    private final int persistentId;

    ValueType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
