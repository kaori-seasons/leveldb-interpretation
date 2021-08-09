package com.complone.base;

import java.io.File;
import java.io.IOException;

public interface DBFactory
{
    DB open(File path, Options options)
            throws IOException;

    void destroy(File path, Options options)
            throws IOException;

    void repair(File path, Options options)
            throws IOException;
}
