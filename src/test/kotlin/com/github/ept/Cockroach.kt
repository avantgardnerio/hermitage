package com.github.ept

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Cockroach : Base(
    org.postgresql.Driver(),
    "jdbc:postgresql://127.0.0.1:26257/defaultdb?sslmode=disable&user=root"
) {
// https://www.cockroachlabs.com/docs/releases/index.html#v22-2
// ./cockroach start-single-node --insecure

    @BeforeEach
    fun setup() {
        stmts[0].execute("drop table if exists test")
        stmts[0].execute("create table test (id int primary key, value int)")
        stmts[0].executeUpdate("insert into test (id, value) values (1, 10), (2, 20)")
    }

    @AfterEach
    fun cleanup() {
        stmts.forEach { it.execute("abort") }
    }

    // --------------------- general
    @Test
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

    // https://sitano.github.io/theory/databases/2019/07/30/tx-isolation-anomalies/#g1c-circular-information-flow
    @Test // fail
    fun `g1c - circular information flow`() {
        execute("begin; -- T1")
        execute("begin; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 22 where id = 2; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Still shows 2 => 20")
        assertQuery("select * from test where id = 1; -- T2. Still shows 1 => 10") // blocks forever
        execute("commit; -- T1")
        var ex: Exception? = null
        try {
            execute("commit; -- T2")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("failed preemptive refresh"))
    }

}