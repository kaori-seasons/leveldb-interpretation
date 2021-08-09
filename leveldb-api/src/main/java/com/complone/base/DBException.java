package com.complone.base;

public class DBException
        extends RuntimeException
{
    public DBException()
    {
    }

    public DBException(String s)
    {
        super(s);
    }

    public DBException(String s, Throwable throwable)
    {
        super(s, throwable);
    }

    public DBException(Throwable throwable)
    {
        super(throwable);
    }
}
