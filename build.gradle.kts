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
    mainClassName = "ru.bmstu.ApplicationStarter"
    applicationDefaultJvmArgs = listOf("-Xms8192m", "-Xmx8192m")
}

tasks.installDist {
    doLast {
        copy {
            from(file("$buildDir/install/${rootProject.name}"))
            into(file("${project.projectDir}/script"))
        }
    }
}
