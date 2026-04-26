// kronos-cli: command-line client. Depends only on common.
plugins {
    application
}

dependencies {
    implementation(project(":kronos-common"))
}

application {
    mainClass.set("io.kronos.cli.KronosCli")
}
