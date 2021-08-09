package com.complone.base.impl;


public final class LogReporters
{
    public static Reporter throwExceptionMonitor()
    {
        return new Reporter()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes", bytes), reason);
            }
        };
    }

    public static Reporter logReporter()
    {
        return new Reporter()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                System.out.println(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                System.out.println(String.format("corruption of %s bytes", bytes));
                reason.printStackTrace();
            }
        };
    }

    private LogReporters()
    {
    }
}
