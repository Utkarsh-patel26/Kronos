// kronos-server: HTTP API + node bootstrap. Depends on all core modules.
plugins {
    application
}

dependencies {
    implementation(project(":kronos-common"))
    implementation(project(":kronos-network"))
    implementation(project(":kronos-raft"))
    implementation(project(":kronos-storage"))
}

application {
    mainClass.set("io.kronos.server.KronosServer")
}
