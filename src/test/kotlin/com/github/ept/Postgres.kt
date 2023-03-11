package com.github.ept

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Postgres {
    val cons: Array<Connection>
    val stmts: Array<Statement>

    init {
        val driver = org.postgresql.Driver()
        val url = "jdbc:postgresql://localhost:5432/postgres"
        val props = Properties()
        props.setProperty("user", "postgres")
        props.setProperty("password", "")
        cons = (0 until 3).map { driver.connect(url, props)!! }.toTypedArray()
        stmts = cons.map { it.createStatement() }.toTypedArray()
    }

    @BeforeEach
    fun setup() {
        stmts[0].execute("drop table if exists test")
        stmts[0].execute("create table test (id int primary key, value int)")
        stmts[0].executeUpdate("insert into test (id, value) values (1, 10), (2, 20)")
    }

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
    fun `pmp repeatable read write predicate`() {
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
    fun g1c() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        execute("update test set value = 11 where id = 1; -- T1")
        execute("update test set value = 22 where id = 2; -- T2")
        assertQuery("select * from test where id = 2; -- T1. Still shows 2 => 20")
        assertQuery("select * from test where id = 1; -- T2. Still shows 1 => 10")
        execute("commit; -- T1")
        execute("commit; -- T2")
    }

    @Test
    fun pmpReadCommitted() {
        execute("begin; set transaction isolation level read committed; -- T1")
        execute("begin; set transaction isolation level read committed; -- T2")
        assertQuery("select * from test where value = 30; -- T1. Returns nothing") // TODO: assert nothing
        execute("insert into test (id, value) values(3, 30); -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- T1. Returns the newly inserted row returns 3 => 30")
        execute("commit; -- T1")
    }

    @Test
    fun pmpRepeatableRead() {
        execute("begin; set transaction isolation level repeatable read; -- T1")
        execute("begin; set transaction isolation level repeatable read; -- T2")
        assertQuery("select * from test where value = 30; -- T1. Returns nothing")
        execute("insert into test (id, value) values(3, 30); -- T2")
        execute("commit; -- T2")
        assertQuery("select * from test where value % 3 = 0; -- T1. Still returns nothing")
        execute("commit; -- T1")
    }

    // --- helpers
    fun execute(sql: String) {
        val stmts = getStatements(sql)
        stmts.forEach { stmt ->
            stmt.execute(sql)
        }
    }

    fun getStatements(sql: String): Array<Statement> {
        val re = """--\s*(\w+)""".toRegex()
        val stmtIdx = re.find(sql)!!.groups[1]!!.value
        val stmts = when (stmtIdx) {
            "T1" -> arrayOf(stmts[0])
            "T2" -> arrayOf(stmts[1])
            "T3" -> arrayOf(stmts[2])
            "either" -> arrayOf(stmts[0], stmts[1])
            else -> throw Exception("Invalid transaction number!")
        }
        return stmts
    }

    fun assertQuery(sql: String) {
        val expected = commentToMap(sql)
        val stmts = getStatements(sql)
        stmts.forEach { stmt ->
            val actual = queryToMap(stmt, sql)
            val compare = when (sql.lowercase(Locale.getDefault()).contains("returns nothing")) {
                true -> actual
                false -> actual.filterKeys { expected.containsKey(it) }
            }
            assertEquals(expected, compare)
        }
    }

    fun queryToMap(stmt: Statement, sql: String): Map<Int, Int> {
        val res = mutableMapOf<Int, Int>()
        val stmts = sql.split(";")
        stmt.executeQuery(stmts.first()).use { rs ->
            while (rs.next()) {
                val k = rs.getInt("id")
                val v = rs.getInt("value")
                res[k] = v
            }
        }
        return res
    }

    fun commentToMap(sql: String): Map<Int, Int> {
        val parts = sql.split("--")
        if (parts.size < 2) return mapOf()
        val comment = parts[1]
        val re = """(?<k>\d+)\s*=>\s*(?<v>\d+)""".toRegex()
        val res = re.findAll(comment).map { res ->
            val k = res.groups["k"]!!.value.toInt()
            val v = res.groups["v"]!!.value.toInt()
            k to v
        }.toMap()
        return res
    }

}