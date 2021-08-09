package com.complone.base.impl;

/**
 * 追加写入的日志条数
 */
public interface LogMonitor {
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
