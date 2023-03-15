package com.github.ept

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Postgres : Base(
    org.postgresql.Driver(),
    "jdbc:postgresql://localhost:5432/postgres?user=postgres&password="
) {
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
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
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
        execute("commit; -- T2")
        assertQuery("select * from test; -- either. Shows 1 => 12, 2 => 22")
    }

    @Test
    fun g1a() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("update test set value = 101 where id = 1; -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("abort;  -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("commit; -- T2")
    }

    @Test
    fun g1b() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("update test set value = 101 where id = 1; -- T1")
        assertQuery("select * from test; -- T2. Still shows 1 => 10")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("commit; -- T1")
        assertQuery("select * from test; -- T2. Now shows 1 => 11")
        execute("commit; -- T2")
    }

    @Test
    fun `g1c - circular information flow should be prevented`() {
        execute("begin; set transaction isolation level serializable; -- T1")
        execute("begin; set transaction isolation level serializable; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 22 where id = 2; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Still shows 2 => 20")
        assertQuery("select * from test where id = 1; -- T2. Still shows 1 => 10")
        execute("commit; -- T1")
        var ex: Exception? = null
        try {
            execute("commit; -- T2")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to read/write dependencies"))
    }

    // ------------------- otv
    @Test
    fun otv() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("begin; set transaction isolation level read committed; -- T3")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 19 where id = 2; -- T1")

        val t2 = Thread {
            execute("update test set value = 12 where id = 1; -- T2. BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2")
        t2.join()

        assertQuery("select * from test where id = 1; -- T3. Shows 1 => 11")
        execute("update test set value = 18 where id = 2; -- T2")
        assertQuery("select * from test where id = 2; -- T3. Shows 2 => 19")
        execute("commit; -- T2")
        assertQuery("select * from test where id = 2; -- T3. Shows 2 => 18")
        assertQuery("select * from test where id = 1; -- T3. Shows 1 => 12")
        execute("commit; -- T3")
    }

    // ------------------------- pmp
    @Test
    fun `pmp - ReadCommitted write predicate`() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("update test set value = value + 10; -- T1")
        val t2 = Thread {
            execute("delete from test where value = 20;  -- T2, BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2")
        t2.join()

        assertQuery("select * from test where value = 20; -- T2, returns 1 => 20 (despite ostensibly having been deleted)")
        execute("commit; -- T2")
    }

    @Test
    fun `pmp - RepeatableRead write predicate`() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("update test set value = value + 10; -- T1")

        val t2 = Thread {
            execute("delete from test where value = 20;  -- T2, BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2")
        t2.join()

        assertQuery("select * from test where value = 20; -- T2, returns 1 => 20 (despite ostensibly having been deleted)")
        execute("commit; -- T2")
    }

    @Test
    fun `p4 - ReadCommitted`() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("select * from test where id = 1; -- T1")
        execute("select * from test where id = 1; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")

        val t2 = Thread {
            execute("update test set value = 11 where id = 1; -- T2, BLOCKS")
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. This unblocks T2, so T1's update is overwritten")
        t2.join()

        execute("commit; -- T2")
        assertQuery("select * from test where id = 1; -- either 1 => 11")
    }

    @Test
    fun `p4 - RepeatableRead`() {
        val wasCalled = AtomicBoolean(false)
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("select * from test where id = 1; -- T1")
        execute("select * from test where id = 1; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")

        var ex: Exception? = null
        val t2 = Thread {
            try {
                execute("update test set value = 11 where id = 1; -- T2, BLOCKS")
            } catch (e: Exception) {
                ex = e
            }
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
            // TODO: assertions in thread only log, they don't break the tests!
        }
        t2.start()
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        execute("commit; -- T1. T2 now prints out ERROR: could not serialize access due to concurrent update")
        t2.join()
        assertTrue(ex!!.message!!.contains("could not serialize access due to concurrent update"))

        execute("abort;  -- T2. There's nothing else we can do, this transaction has failed")
    }

    @Test
    fun `pmp - ReadCommitted`() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        assertQuery("select * from test where value = 30; -- T1. Returns nothing") // TODO: assert nothing
        execute("insert into test (id, value) values(3, 30); -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- T1. Returns the newly inserted row returns 3 => 30")
        execute("commit; -- T1")
    }

    @Test
    fun `pmp - RepeatableRead`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        assertQuery("select * from test where value = 30; -- T1. Returns nothing")
        execute("insert into test (id, value) values(3, 30); -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- T1. Still returns nothing")
        execute("commit; -- T1")
    }

    @Test
    fun `G-single - ReadCommitted does not prevent Read Skew`() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        assertQuery("select * from test where id = 1; -- T1. Shows 1 => 10")
        execute("select * from test where id = 1; -- T2")
        execute("select * from test where id = 2; -- T2")
        execute("update test set value = 12 where id = 1; -- T2")
        execute("update test set value = 18 where id = 2; -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Shows 2 => 18")
        execute("commit; -- T1")
    }

    @Test
    fun `G-single - RepeatableRead prevents Read Skew`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        assertQuery("select * from test where id = 1; -- T1. Shows 1 => 10")
        execute("select * from test where id = 1; -- T2")
        execute("select * from test where id = 2; -- T2")
        execute("update test set value = 12 where id = 1; -- T2")
        execute("update test set value = 18 where id = 2; -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Shows 2 => 20")
        execute("commit; -- T1")
    }

    @Test
    fun `G-single - RepeatableRead prevents Read Skew with predicate`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("select * from test where value % 5 = 0; -- T1")
        execute("update test set value = 12 where value = 10; -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- T1. Returns nothing")
        execute("commit; -- T1")
    }

    @Test
    fun `G-single - RepeatableRead prevents Read Skew with write predicate`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("select * from test where id = 1; -- T1. Shows 1 => 10")
        execute("select * from test; -- T2")
        execute("update test set value = 12 where id = 1; -- T2")
        execute("update test set value = 18 where id = 2; -- T2")
        execute("commit; -- T2")
        var ex: Exception? = null
        try {
            execute("delete from test where value = 20; -- T1. Prints ERROR: could not serialize access due to concurrent update")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to concurrent update"))
        execute("abort; -- T1. There's nothing else we can do, this transaction has failed")
    }

    @Test
    fun `G2-item - RepeatableRead does not prevent Write Skew`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("select * from test where id in (1,2); -- T1")
        execute("select * from test where id in (1,2); -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 21 where id = 2; -- T2")
        execute("commit; -- T1")
        execute("commit; -- T2")
        assertQuery("select * from test; -- either Shows 1 => 11, 2 => 21")
    }

    @Test
    fun `G2-item - Serializable prevents write skew`() {
        execute("begin; set transaction isolation level serializable; -- T1")
        execute("begin; set transaction isolation level serializable; -- T2")
        execute("select * from test where id in (1,2); -- T1")
        execute("select * from test where id in (1,2); -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 21 where id = 2; -- T2")
        execute("commit; -- T1")
        var ex: Exception? = null
        try {
            execute("commit; -- T2. Prints out ERROR: could not serialize access due to read/write dependencies among transactions")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to read/write dependencies"))
    }

    @Test
    fun `G2 - RepeatableRead does not prevent anti-dependency cycles`() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        execute("select * from test where value % 3 = 0; -- T1")
        execute("select * from test where value % 3 = 0; -- T2")
        execute("insert into test (id, value) values(3, 30); -- T1")
        execute("insert into test (id, value) values(4, 42); -- T2")
        execute("commit; -- T1")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42")
    }

    @Test
    fun `G2 - Serializable prevents Anti-Dependency Cycles`() {
        execute("begin; set transaction isolation level serializable; -- T1")
        execute("begin; set transaction isolation level serializable; -- T2")
        execute("select * from test where value % 3 = 0; -- T1")
        execute("select * from test where value % 3 = 0; -- T2")
        execute("insert into test (id, value) values(3, 30); -- T1")
        execute("insert into test (id, value) values(4, 42); -- T2")
        execute("commit; -- T1")
        var ex: Exception? = null
        try {
            execute("commit; -- T2. Prints out ERROR: could not serialize access due to read/write dependencies among transactions")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to read/write dependencies"))
    }

    @Test
    fun `G2 - Serializable prevents 2 edge Anti-Dependency Cycles`() {
        execute("begin; set transaction isolation level serializable; -- T1")
        execute("select * from test; -- T1. Shows 1 => 10, 2 => 20")
        execute("begin; set transaction isolation level serializable; -- T2")
        execute("update test set value = value + 5 where id = 2; -- T2")
        execute("commit; -- T2")
        execute("begin; set transaction isolation level serializable; -- T3")
        execute("select * from test; -- T3. Shows 1 => 10, 2 => 25")
        execute("commit; -- T3")
        var ex: Exception? = null
        try {
            execute("update test set value = 0 where id = 1; -- T1. Prints out ERROR: could not serialize access due to read/write dependencies among transactions")
        } catch (e: Exception) {
            ex = e
        }
        assertTrue(ex!!.message!!.contains("could not serialize access due to read/write dependencies"))
        execute("abort; -- T1. There's nothing else we can do, this transaction has failed")
    }

}