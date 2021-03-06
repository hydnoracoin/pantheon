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
package tech.pegasys.pantheon.ethereum.p2p.peers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import io.vertx.core.json.DecodeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StaticNodesParserTest {

  // NOTE: The invalid_static_nodes file is identical to the valid, however one node's port is set
  // to "A".

  // First peer ion the valid_static_nodes file.
  private final List<EnodeURL> validFileItems =
      Lists.newArrayList(
          new EnodeURL(
              "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa",
              "127.0.0.1",
              30303),
          new EnodeURL(
              "02beb46bc17227616be44234071dfa18516684e45eed88049190b6cb56b0bae218f045fd0450f123b8f55c60b96b78c45e8e478004293a8de6818aa4e02eff97",
              "127.0.0.1",
              30304),
          new EnodeURL(
              "819e5cbd81f123516b10f04bf620daa2b385efef06d77253148b814bf1bb6197ff58ebd1fd7bf5dc765b49a4440c733bf941e479c800173f2bfeb887e4fbcbc2",
              "127.0.0.1",
              30305),
          new EnodeURL(
              "6cf53e25d2a98a22e7e205a86bda7077e3c8a7bc99e5ff88ddfd2037a550969ab566f069ffa455df0cfae0c21f7aec3447e414eccc473a3e8b20984b90f164ac",
              "127.0.0.1",
              30306));

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void validFileLoadsWithExpectedEnodes() throws IOException {
    final URL resource = StaticNodesParserTest.class.getResource("valid_static_nodes.json");
    final Path path = Paths.get(resource.getPath());

    final Set<EnodeURL> enodes = StaticNodesParser.fromPath(path);

    assertThat(enodes).containsExactly(validFileItems.toArray(new EnodeURL[validFileItems.size()]));
  }

  @Test
  public void invalidFileThrowsAnException() {
    final URL resource = StaticNodesParserTest.class.getResource("invalid_static_nodes.json");
    final Path path = Paths.get(resource.getPath());

    assertThatThrownBy(() -> StaticNodesParser.fromPath(path))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nonJsonFileThrowsAnException() throws IOException {
    final File tempFile = testFolder.newFile("file.txt");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), "This Is Not Json".getBytes(Charset.forName("UTF-8")));

    assertThatThrownBy(() -> StaticNodesParser.fromPath(tempFile.toPath()))
        .isInstanceOf(DecodeException.class);
  }

  @Test
  public void anEmptyCacheIsCreatedIfTheFileDoesNotExist() throws IOException {
    final Path path = Paths.get("./arbirtraryFilename.txt");

    final Set<EnodeURL> enodes = StaticNodesParser.fromPath(path);
    assertThat(enodes.size()).isZero();
  }

  @Test
  public void cacheIsCreatedIfFileExistsButIsEmpty() throws IOException {
    final File tempFile = testFolder.newFile("file.txt");
    tempFile.deleteOnExit();

    final Set<EnodeURL> enodes = StaticNodesParser.fromPath(tempFile.toPath());
    assertThat(enodes.size()).isZero();
  }
}
