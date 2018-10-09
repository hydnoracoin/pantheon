package net.consensys.pantheon.ethereum.jsonrpc.internal.methods;

import net.consensys.pantheon.ethereum.jsonrpc.internal.JsonRpcRequest;
import net.consensys.pantheon.ethereum.jsonrpc.internal.parameters.BlockParameter;
import net.consensys.pantheon.ethereum.jsonrpc.internal.parameters.JsonRpcParameter;
import net.consensys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import net.consensys.pantheon.ethereum.jsonrpc.internal.results.Quantity;

public class EthGetUncleCountByBlockNumber extends AbstractBlockParameterMethod {

  public EthGetUncleCountByBlockNumber(
      final BlockchainQueries blockchain, final JsonRpcParameter parameters) {
    super(blockchain, parameters);
  }

  @Override
  public String getName() {
    return "eth_getUncleCountByBlockNumber";
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequest request) {
    return parameters().required(request.getParams(), 0, BlockParameter.class);
  }

  @Override
  protected String resultByBlockNumber(final JsonRpcRequest request, final long blockNumber) {
    return blockchainQueries().getOmmerCount(blockNumber).map(Quantity::create).orElse(null);
  }
}