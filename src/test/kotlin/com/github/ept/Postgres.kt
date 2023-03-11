package com.github.ept

import org.junit.jupiter.api.Test
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class Postgres {
    @Test
    fun g0() {
        val driver = org.postgresql.Driver()
        val url = "jdbc:postgresql://localhost:5432/postgres"
        val props = Properties()
        props.setProperty("user", "postgres")
        props.setProperty("password", "")
        driver.connect(url, props).use { con ->
            assertNotNull(con)
            con.createStatement().use { stmt ->
                assertEquals(0, stmt.executeUpdate("drop table if exists test"))
                assertEquals(0, stmt.executeUpdate("create table test (id int primary key, value int)"))
                assertEquals(2, stmt.executeUpdate("insert into test (id, value) values (1, 10), (2, 20)"))
            }
        }
        assert(true)
    }
}