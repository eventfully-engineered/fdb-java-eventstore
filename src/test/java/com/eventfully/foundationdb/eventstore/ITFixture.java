package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ExecutionException;

// @Testcontainers
abstract class ITFixture extends TestFixture {

    private static final String FDB_NETWORKING_MODE_KEY = "FDB_NETWORKING_MODE";
    private static final int FDB_PORT = 4689;

//    @Container
//    private static FixedHostPortGenericContainer<?> fdbContainer = new FixedHostPortGenericContainer<>("foundationdb/foundationdb:6.3.5")
//        .withFixedExposedPort(FDB_PORT, FDB_PORT)
//        .withExposedPorts(FDB_PORT)
//        .withEnv(FDB_NETWORKING_MODE_KEY, "host")
//        .withFileSystemBind("./etc", "/etc/foundationdb")
//        .waitingFor(Wait.forLogMessage(".*FDBD joined cluster.*\\n", 1));


    protected FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(620);
        TestHelpers.clean(fdb);
    }

}
