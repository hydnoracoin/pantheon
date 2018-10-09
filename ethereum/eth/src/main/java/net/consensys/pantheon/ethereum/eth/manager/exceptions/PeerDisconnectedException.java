package net.consensys.pantheon.ethereum.eth.manager.exceptions;

public class PeerDisconnectedException extends EthTaskException {

  public PeerDisconnectedException() {
    super(FailureReason.PEER_DISCONNECTED);
  }
}