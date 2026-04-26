rootProject.name = "kronos"

include(
    "kronos-common",
    "kronos-network",
    "kronos-raft",
    "kronos-storage",
    "kronos-server",
    "kronos-gui",
    "kronos-cli"
)

// Gradle auto-loads gradle/libs.versions.toml as the "libs" catalog.
