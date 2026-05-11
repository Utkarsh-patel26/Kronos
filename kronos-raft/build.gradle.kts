// kronos-raft: consensus engine. Depends on common, network, and storage.
dependencies {
    implementation(project(":kronos-common"))
    implementation(project(":kronos-network"))
    implementation(project(":kronos-storage"))
}
