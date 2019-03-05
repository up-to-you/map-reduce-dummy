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
}
