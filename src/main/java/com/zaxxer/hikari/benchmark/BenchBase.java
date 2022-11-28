/*
 * Copyright (C) 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.benchmark;

import com.alibaba.druid.filter.stat.MergeStatFilter;
import com.alibaba.druid.filter.stat.StatFilter;
import com.alibaba.druid.pool.DruidDataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import one.datasource.DataSourceImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.vibur.dbcp.ViburDBCPDataSource;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executors;

@State(Scope.Benchmark)
public class BenchBase
{
    public static final String DRIVER_CLASS_NAME = "com.zaxxer.hikari.benchmark.stubs.StubDriver";
    public static final String USERNAME = "brettw";
    public static final String PASSWORD = "";
    public static final int CONNECT_TIMEOUT = 8000;
    public  static final boolean VALIDATE_ON_BORROW = true;
    public static final String VALIDATION_QUERY = "SELECT 1";
    public static final int VALIDATION_INTERVAL = 1000;
    public static final int MIN_POOL_SIZE = 0;
    
    //    @Param({ "hikari", "dbcp2", "tomcat", "c3p0", "vibur", "druid", "druid-stat", "druid-stat-merge", "ucp" })
    @Param({ "hikari", "ucp" })
    public String pool;

    @Param({ "32" })
    public int maxPoolSize;

    @Param({ "jdbc:stub" })
    public String jdbcUrl;

    public static DataSource DS;

    @Setup(Level.Trial)
    public void setup(BenchmarkParams params)
    {
        try
        {
            Class.forName(DRIVER_CLASS_NAME);
            System.err.printf("Using driver (%s): %s", jdbcUrl, DriverManager.getDriver(jdbcUrl));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (this.getClass().getName().contains("Statement")) {
            System.err.println("# Overriding maxPoolSize paramter for StatementBench: maxPoolSize=" + params.getThreads());
            maxPoolSize = params.getThreads();
        }

        switch (pool)
        {
        case "hikari":
            setupHikari();
            break;
        case "tomcat":
            setupTomcat();
            break;
        case "dbcp":
            setupDbcp();
            break;
        case "dbcp2":
            setupDbcp2();
            break;
        case "c3p0":
            setupC3P0();
            break;
        case "vibur":
            setupVibur();
            break;
        case "one":
            setupOne();
            break;
        case "druid":
            setupDruid();
            break;
        case "druid-stat":
            setupDruidStat();
            break;
        case "druid-stat-merge":
            setupDruidStatMerge();
            break;
        case "ucp":
            setupUcp();
            break;
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws SQLException
    {
        switch (pool)
        {
        case "hikari":
            ((HikariDataSource) DS).close();
            break;
        case "tomcat":
            ((org.apache.tomcat.jdbc.pool.DataSource) DS).close();
            break;
        case "dbcp":
            ((org.apache.commons.dbcp.BasicDataSource) DS).close();
            break;
        case "dbcp2":
            ((BasicDataSource) DS).close();
            break;
        case "c3p0":
            ((ComboPooledDataSource) DS).close();
            break;
        case "vibur":
            ((ViburDBCPDataSource) DS).terminate();
            break;
        case "druid":
            ((DruidDataSource) DS).close();
            break;
        case "druid-stat":
            ((DruidDataSource) DS).close();
            break;
        case "druid-stat-merge":
            ((DruidDataSource) DS).close();
            break;

        }
    }

    private void setupUcp() {
        try {
            PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
            pds.setConnectionFactoryClassName(DRIVER_CLASS_NAME);
            pds.setUser(USERNAME);
            pds.setPassword(PASSWORD);
            pds.setURL(jdbcUrl);
            pds.setInitialPoolSize(MIN_POOL_SIZE);
            pds.setMaxPoolSize(maxPoolSize);
            pds.setValidateConnectionOnBorrow(VALIDATE_ON_BORROW);
            pds.setSQLForValidateConnection(VALIDATION_QUERY);
            pds.setSecondsToTrustIdleConnection(VALIDATION_INTERVAL);

            DS = pds;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setupDruid() {
        DS = createDruid();
    }

    protected void setupDruidStat()
    {
        DruidDataSource druid = createDruid();

        try {
            druid.addFilters("stat");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        DS = druid;
    }

    protected void setupDruidStatMerge()
    {
        DruidDataSource druid = createDruid();

        StatFilter statFilter = new MergeStatFilter();
        druid.getProxyFilters().add(statFilter);
        DS = druid;
    }

    protected DruidDataSource createDruid()
    {
        DruidDataSource druid = new DruidDataSource();

        druid.setInitialSize(MIN_POOL_SIZE);
        druid.setMaxActive(maxPoolSize);
        druid.setMinIdle(MIN_POOL_SIZE);
        druid.setPoolPreparedStatements(true);
        druid.setDriverClassName(DRIVER_CLASS_NAME);
        druid.setUrl(jdbcUrl);
        druid.setUsername(USERNAME);
        druid.setPassword(PASSWORD);
        druid.setValidationQuery(VALIDATION_QUERY);
        druid.setTestOnBorrow(VALIDATE_ON_BORROW);
        druid.setDefaultAutoCommit(false);
        druid.setMaxWait(CONNECT_TIMEOUT);
        druid.setUseUnfairLock(true);

        return druid;
    }

    protected void setupTomcat()
    {
        PoolProperties props = new PoolProperties();
        props.setUrl(jdbcUrl);
        props.setDriverClassName(DRIVER_CLASS_NAME);
        props.setUsername(USERNAME);
        props.setPassword(PASSWORD);
        props.setInitialSize(MIN_POOL_SIZE);
        props.setMinIdle(MIN_POOL_SIZE);
        props.setMaxIdle(maxPoolSize);
        props.setMaxActive(maxPoolSize);
        props.setMaxWait(CONNECT_TIMEOUT);

        props.setDefaultAutoCommit(false);

        props.setRollbackOnReturn(true);
        props.setUseDisposableConnectionFacade(true);
        props.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState"); 
        props.setTestOnBorrow(VALIDATE_ON_BORROW);
        props.setValidationInterval(VALIDATION_INTERVAL);
        props.setValidator((connection, validateAction) -> {
            try {
                return (validateAction != PooledConnection.VALIDATE_BORROW || connection.isValid(0));
            }
            catch (SQLException e)
            {
                return false;
            }
        });

        DS = new org.apache.tomcat.jdbc.pool.DataSource(props);
    }

    protected void setupDbcp()
    {
        org.apache.commons.dbcp.BasicDataSource ds = new org.apache.commons.dbcp.BasicDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUsername(USERNAME);
        ds.setPassword(PASSWORD);
        ds.setInitialSize(MIN_POOL_SIZE);
        ds.setMinIdle(MIN_POOL_SIZE);
        ds.setMaxIdle(maxPoolSize);
        ds.setMaxActive(maxPoolSize);

        ds.setDefaultAutoCommit(false);
        ds.setTestOnBorrow(VALIDATE_ON_BORROW);
        ds.setValidationQuery(VALIDATION_QUERY);

        DS = ds;
    }

    protected void setupDbcp2()
    {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUsername(USERNAME);
        ds.setPassword(PASSWORD);
        ds.setInitialSize(MIN_POOL_SIZE);
        ds.setMinIdle(MIN_POOL_SIZE);
        ds.setMaxIdle(maxPoolSize);
        ds.setMaxTotal(maxPoolSize);
        ds.setMaxWaitMillis(CONNECT_TIMEOUT);

        ds.setDefaultAutoCommit(false);
        ds.setRollbackOnReturn(true);
        ds.setEnableAutoCommitOnReturn(false);
        ds.setTestOnBorrow(VALIDATE_ON_BORROW);
        ds.setCacheState(true);
        ds.setFastFailValidation(true);

        DS = ds;
    }

    protected void setupHikari()
    {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setMinimumIdle(MIN_POOL_SIZE);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(CONNECT_TIMEOUT);
        config.setAutoCommit(false);

        DS = new HikariDataSource(config);
    }

    protected void setupC3P0()
    {
        try
        {
            ComboPooledDataSource cpds = new ComboPooledDataSource();
            cpds.setJdbcUrl( jdbcUrl );
            cpds.setUser(USERNAME);
            cpds.setPassword(PASSWORD);
            cpds.setAcquireIncrement(1);
            cpds.setInitialPoolSize(MIN_POOL_SIZE);
            cpds.setMinPoolSize(MIN_POOL_SIZE);
            cpds.setMaxPoolSize(maxPoolSize);
            cpds.setCheckoutTimeout(CONNECT_TIMEOUT);
            cpds.setLoginTimeout(8);
            cpds.setTestConnectionOnCheckout(VALIDATE_ON_BORROW);

            DS = cpds;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void setupVibur()
    {
        ViburDBCPDataSource vibur = new ViburDBCPDataSource();
        vibur.setJdbcUrl( jdbcUrl );
        vibur.setUsername(USERNAME);
        vibur.setPassword(PASSWORD);
        vibur.setConnectionTimeoutInMs(CONNECT_TIMEOUT);
        vibur.setValidateTimeoutInSeconds(VALIDATION_INTERVAL);
        vibur.setLoginTimeoutInSeconds(2);
        vibur.setPoolInitialSize(MIN_POOL_SIZE);
        vibur.setPoolMaxSize(maxPoolSize);
        vibur.setConnectionIdleLimitInSeconds(1);
        vibur.setAcquireRetryAttempts(0);
        vibur.setReducerTimeIntervalInSeconds(0);
        vibur.setUseNetworkTimeout(true);
        vibur.setNetworkTimeoutExecutor(Executors.newFixedThreadPool(1));
        vibur.setClearSQLWarnings(true);
        vibur.setResetDefaultsAfterUse(true);
        vibur.start();

        DS = vibur;
    }

    private void setupOne()
    {
        Properties props = new Properties();
        props.put("url", jdbcUrl);
        props.put("driver", DRIVER_CLASS_NAME);

        DS = new DataSourceImpl("one", props);
    }
}
