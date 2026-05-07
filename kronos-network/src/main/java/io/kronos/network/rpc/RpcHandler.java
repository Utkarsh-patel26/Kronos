package io.kronos.network.rpc;

/**
 * Handler for one Raft RPC message type.
 *
 * <p>Called on the Raft event-queue thread with the raw request body bytes.
 * Returns the raw response body bytes that will be sent back to the caller.
 */
@FunctionalInterface
public interface RpcHandler {
    byte[] handle(byte[] body);
}
