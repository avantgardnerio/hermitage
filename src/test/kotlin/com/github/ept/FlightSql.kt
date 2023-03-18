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

    // https://sitano.github.io/theory/databases/2019/07/30/tx-isolation-anomalies/#g0-dirty-writes
    @Test // pass
    fun g0() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; -- T1")
        execute("begin; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")

        val t2 = Thread {
            execute("update test set value = 12 where id = 1; -- T2, BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        Thread.sleep(500)
        execute("update test set value = 21 where id = 2; -- T1")
        assertQuery("select * from test; -- T1. Shows 1 => 11, 2 => 21")
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2")
        t2.join()

        execute("update test set value = 22 where id = 2; -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test; -- either. Shows 1 => 12, 2 => 22")
    }

    // https://sitano.github.io/theory/databases/2019/07/30/tx-isolation-anomalies/#g1a-aborted-reads-cascaded-aborts
    @Test // pass
    fun `g1a - aborted reads should be prevented`() {
        execute("begin transaction isolation level serializable; -- T1")
        execute("begin transaction isolation level serializable; -- T2")
        execute("update test set value = 101 where id = 1; -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("rollback;  -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("commit; -- T2")
    }

    // https://sitano.github.io/theory/databases/2019/07/30/tx-isolation-anomalies/#g1b-intermediate-reads-dirty-reads
    @Test // pass
    fun `g1b - intermediate reads should be prevented`() {
        execute("begin transaction isolation level serializable; -- T1")
        execute("begin transaction isolation level serializable; -- T2")
        execute("update test set value = 101 where id = 1; -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("commit; -- T1")
        assertQuery("select * from test; -- T2. Now shows 1 => 11")
        execute("commit; -- T2")
    }

    // https://sitano.github.io/theory/databases/2019/07/30/tx-isolation-anomalies/#g1c-circular-information-flow
    @Test
    fun `g1c - circular information flow should be prevented`() {
        execute("begin transaction isolation level serializable; -- T1")
        execute("begin transaction isolation level serializable; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 22 where id = 2; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Still shows 2 => 20") // mark conflict due to newer version
        assertQuery("select * from test where id = 1; -- T2. Still shows 1 => 10") // mark conflict due to newer version
        execute("commit; -- T1") // not both in and out? commit?
        var ex: Exception? = null
        try {
            execute("commit; -- T2") // in and out marked - rollback
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to read/write dependencies"))
    }

}