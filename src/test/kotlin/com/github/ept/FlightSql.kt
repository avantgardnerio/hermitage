package com.github.ept

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FlightSql : Base(
    ArrowFlightJdbcDriver(),
    "jdbc:arrow-flight://127.0.0.1:50060?useEncryption=false&user=admin&password=password"
) {
    @BeforeEach
    fun start() {
        stmts[0].execute("create table test (id int, value int, primary key (id))")
        stmts[0].executeUpdate("insert into test (id, value) values (1, 10), (2, 20)")
    }

    @Test
    fun g0() {
        val wasCalled = AtomicBoolean(false)
        execute("begin transaction isolation level serializable; -- T1")
        execute("begin transaction isolation level serializable; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")

        val t2 = Thread {
            execute("update test set value = 12 where id = 1; -- T2, BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        execute("update test set value = 21 where id = 2; -- T1")
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2")
        t2.join()

        assertQuery("select * from test; -- T1. Shows 1 => 11, 2 => 21")
        execute("update test set value = 22 where id = 2; -- T2")
        execute("commit; -- T2") // 'Custom("Mutation failed due to concurrent update at src/server.rs:368")' at src/server.rs:797
        assertQuery("select * from test; -- either. Shows 1 => 12, 2 => 22")
    }
}