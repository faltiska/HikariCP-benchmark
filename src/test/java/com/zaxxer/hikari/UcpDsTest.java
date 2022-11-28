package com.zaxxer.hikari;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.junit.Assert;
import org.junit.Test;

public class UcpDsTest {
    @Test
    public void testUcpDs() throws Exception {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName("com.zaxxer.hikari.benchmark.stubs.StubDriver");
        pds.setUser("brett");
        pds.setPassword("");
        pds.setURL("jdbc:stub");
        pds.setInitialPoolSize(0);
        pds.setMaxPoolSize(32);
        pds.setValidateConnectionOnBorrow(true);
        pds.setSecondsToTrustIdleConnection(1000);

        Assert.assertNotNull(pds.getConnection());
    }
}
