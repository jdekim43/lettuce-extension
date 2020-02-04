import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.util.Date

plugins {
    kotlin("jvm") version "1.3.61"
    `maven-publish`
    id("com.jfrog.bintray") version "1.8.4"
}

val artifactName = "lettuce-extension"
val artifactGroup = "kr.jadekim"
val artifactVersion = "0.0.1"
group = artifactGroup
version = artifactVersion

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    val jLoggerVersion: String by project
    val kotlinxCoroutineVersion: String by project
    val lettuceVersion: String by project
    val commonsPool2Version: String by project
    val jacksonVersion: String by project

    implementation(kotlin("stdlib-jdk8"))

    implementation("kr.jadekim:j-logger:$jLoggerVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$kotlinxCoroutineVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")

    implementation("io.lettuce:lettuce-core:$lettuceVersion")

    implementation("org.apache.commons:commons-pool2:$commonsPool2Version")
}

tasks.withType<KotlinCompile> {
    val jvmTarget: String by project

    kotlinOptions.jvmTarget = jvmTarget
}

val sourcesJar by tasks.creating(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.getByName("main").allSource)
}

publishing {
    publications {
        create<MavenPublication>("lib") {
            groupId = artifactGroup
            artifactId = artifactName
            version = artifactVersion
            from(components["java"])
            artifact(sourcesJar)
        }
    }
}

bintray {
    user = System.getenv("BINTRAY_USER")
    key = System.getenv("BINTRAY_KEY")

    publish = true

    setPublications("lib")

    pkg.apply {
        repo = "maven"
        name = rootProject.name
        setLicenses("MIT")
        setLabels("kotlin", "lettuce")
        vcsUrl = "https://github.com/jdekim43/lettuce-extension.git"
        version.apply {
            name = artifactVersion
            released = Date().toString()
        }
    }
}