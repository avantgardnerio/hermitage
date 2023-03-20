package com.github.ept

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MySql : Base(
    com.mysql.cj.jdbc.Driver(),
    "jdbc:mysql://127.0.0.1:3306/mysql?user=root&password=password"
) {

    @BeforeEach
    fun setup() {
        stmts[0].execute("drop table if exists test")
        stmts[0].execute("create table test (id int primary key, value int) engine=innodb;")
        stmts[0].executeUpdate("insert into test (id, value) values (1, 10), (2, 20)")
    }

    @AfterEach
    fun cleanup() {
        stmts.forEach { it.execute("rollback") }
    }

    // --------------------- general
    @Test
    fun `otv - observed values from vanish in read uncommitted`() {
        val wasCalled = AtomicBoolean(false)
        execute("set session transaction isolation level read uncommitted; -- T1")
        execute("set session transaction isolation level read uncommitted; -- T2")
        execute("set session transaction isolation level read uncommitted; -- T3")
        execute("start transaction; -- T1")
        execute("start transaction; -- T2")
        execute("start transaction; -- T3")
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

        assertQuery("select * from test; -- T3. Shows 1 => 12, 2 => 19")
        execute("update test set value = 18 where id = 2; -- T2")
        assertQuery("select * from test; -- T3. Shows 1 => 12, 2 => 18")
    }

}