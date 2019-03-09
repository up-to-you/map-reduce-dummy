import org.gradle.api.JavaVersion.VERSION_11

plugins {
    idea
    java
    application
}

java {
    sourceCompatibility = VERSION_11
    targetCompatibility = VERSION_11
}

repositories {
    jcenter()
}

dependencies {
}

application {
    buildDir = project.projectDir
    mainClassName = "ru.bmstu.ApplicationStarter"
    executableDir = "/"
    applicationDefaultJvmArgs = listOf("-Xms8192m", "-Xmx8192m")
}
