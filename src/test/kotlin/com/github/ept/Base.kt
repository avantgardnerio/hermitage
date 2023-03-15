package com.github.ept

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.sql.Connection
import java.sql.Driver
import java.sql.Statement
import java.util.*
import kotlin.test.assertEquals

open class Base {
    val cons: Array<Connection>
    val stmts: Array<Statement>

    constructor(driver: Driver, url: String) {
        cons = (0 until 3).map { driver.connect(url, Properties())!! }.toTypedArray()
        stmts = cons.map { it.createStatement() }.toTypedArray()
    }

    fun execute(sql: String) {
        val stmts = getStatements(sql)
        stmts.forEach { stmt ->
            stmt.execute(sql)
        }
    }

    fun getStatements(sql: String): Array<Statement> {
        val re = """--\s*(\w+)""".toRegex()
        val stmtIdx = re.find(sql)!!.groups[1]!!.value
        val stmts = when (stmtIdx.lowercase(Locale.getDefault())) {
            "t1" -> arrayOf(stmts[0])
            "t2" -> arrayOf(stmts[1])
            "t3" -> arrayOf(stmts[2])
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
            val compare = when (expected.isEmpty()) {
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