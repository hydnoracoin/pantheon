/*
 * Copyright 2019 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.ethereum.eth.sync.tasks;

import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;

import java.util.List;
import java.util.Objects;

public class BlockWithReceipts {
  private final Block block;
  private final List<TransactionReceipt> receipts;

  public BlockWithReceipts(final Block block, final List<TransactionReceipt> receipts) {
    this.block = block;
    this.receipts = receipts;
  }

  public BlockHeader getHeader() {
    return block.getHeader();
  }

  public Block getBlock() {
    return block;
  }

  public List<TransactionReceipt> getReceipts() {
    return receipts;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final BlockWithReceipts that = (BlockWithReceipts) o;
    return block.equals(that.block) && Objects.equals(receipts, that.receipts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(block, receipts);
  }
}
