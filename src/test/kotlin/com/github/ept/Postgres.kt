package com.github.ept

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Statement
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Postgres {
    val con1: Connection
    val con2: Connection
    val stmt1: Statement
    val stmt2: Statement

    init {
        val driver = org.postgresql.Driver()
        val url = "jdbc:postgresql://localhost:5432/postgres"
        val props = Properties()
        props.setProperty("user", "postgres")
        props.setProperty("password", "")
        con1 = driver.connect(url, props)!!
        con2 = driver.connect(url, props)!!
        stmt1 = con1.createStatement()
        stmt2 = con2.createStatement()
    }

    @BeforeEach
    fun setup() {
        stmt1.execute("drop table if exists test")
        stmt1.execute("create table test (id int primary key, value int)")
        stmt1.executeUpdate("insert into test (id, value) values (1, 10), (2, 20)")
    }

    @Test
    fun g0() {
        val wasCalled = AtomicBoolean(false)
        stmt1.executeUpdate("begin; set transaction isolation level read committed; -- T1")
        stmt2.executeUpdate("begin; set transaction isolation level read committed; -- T2")
        assertEquals(1, stmt1.executeUpdate("update test set value = 11 where id = 1; -- T1"))
        val t2 = Thread {
            assertEquals(1, stmt2.executeUpdate("update test set value = 12 where id = 1; -- T2, BLOCKS"))
            assertTrue(wasCalled.get(), "t1 should have committed before t2 update complete!")
        }
        t2.start()
        assertEquals(1, stmt1.executeUpdate("update test set value = 21 where id = 2; -- T1"))
        Thread.sleep(500)
        assertFalse(wasCalled.getAndSet(true), "t2 should not have updated until t1 commits!")
        stmt1.execute("commit; -- T1. This unblocks T2")
        t2.join()
        val rs = stmt1.executeQuery("select * from test -- T1. Shows 1 => 11, 2 => 21")
        assertTrue(rs.next())
        assertEquals(1, rs.getInt("id"))
        assertEquals(11, rs.getInt("value"))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt("id"))
        assertEquals(21, rs.getInt("value"))
        assertFalse(rs.next())
        assertEquals(1, stmt2.executeUpdate("update test set value = 22 where id = 2; -- T2"))
        stmt2.execute("commit; -- T2")
        arrayOf(stmt1, stmt2).forEach { stmt ->
            val rs = stmt.executeQuery("select * from test -- either. Shows 1 => 12, 2 => 22")
            assertTrue(rs.next())
            assertEquals(1, rs.getInt("id"))
            assertEquals(12, rs.getInt("value"))
            assertTrue(rs.next())
            assertEquals(2, rs.getInt("id"))
            assertEquals(22, rs.getInt("value"))
            assertFalse(rs.next())
        }
    }
}