plugins {
    kotlin("jvm") version "1.8.0"
}

group = "com.github.ept"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.postgresql:postgresql:42.5.4")
    implementation("org.apache.arrow:flight-sql-jdbc-driver:11.0.0")
    implementation("mysql:mysql-connector-java:8.0.32")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}