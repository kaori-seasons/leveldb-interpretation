package com.complone.base.impl;

/**
 * 汇报错误
 */
public interface Reporter
{
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
