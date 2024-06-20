/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.jms;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
abstract class PulsarTemporaryDestination extends PulsarDestination {

  private final PulsarSession session;

  public PulsarTemporaryDestination(String topicName, PulsarSession session)
      throws InvalidDestinationException {
    super(topicName);
    this.session = session;
    if (isVirtualDestination()) {
      throw new InvalidDestinationException("Temporary destinations cannot be virtual");
    }
  }

  public PulsarSession getSession() {
    return session;
  }

  public final void delete() throws JMSException {
    try {
      log.info("Deleting {}", this);
      String topicName = getInternalTopicName();
      String fullQualifiedTopicName = session.getFactory().applySystemNamespace(topicName);

      if (session
          .getFactory()
          .getPulsarAdmin()
          .topics()
          .getPartitionedTopicList(session.getFactory().getSystemNamespace())
          .stream()
          .anyMatch(t -> t.equals(fullQualifiedTopicName))) {

        if (getNumberOfActiveConsumers(fullQualifiedTopicName, true) > 0) {
          throw new JMSException("Cannot delete a temporary destination with active consumers");
        } else {
          session
              .getFactory()
              .getPulsarAdmin()
              .topics()
              .deletePartitionedTopic(
                  fullQualifiedTopicName,
                  session.getFactory().isForceDeleteTemporaryDestinations());
        }
      } else {
        if (getNumberOfActiveConsumers(fullQualifiedTopicName, false) > 0) {
          throw new JMSException("Cannot delete a temporary destination with active consumers");
        } else {
          session
              .getFactory()
              .getPulsarAdmin()
              .topics()
              .delete(
                  fullQualifiedTopicName,
                  session.getFactory().isForceDeleteTemporaryDestinations());
        }
      }

    } catch (final PulsarAdminException paEx) {
      Utils.handleException(paEx);
    } finally {
      session.getConnection().removeTemporaryDestination(this);
    }
  }

  /**
   * To account for lag in the Pulsar Admin metadata, this method will wait a total of 3 seconds for
   * any consumers to be removed from the topic before returning the final count.
   *
   * @param topic
   * @param partitionedTopic
   * @return
   * @throws IllegalStateException
   * @throws PulsarAdminException
   */
  private int getNumberOfActiveConsumers(String topic, boolean partitionedTopic)
      throws IllegalStateException, PulsarAdminException {
    int numConsumers, numAttempts = 0;

    do {
      if (partitionedTopic) {
        PartitionedTopicStats partitionedStats =
            session.getFactory().getPulsarAdmin().topics().getPartitionedStats(topic, true);

        log.info("Partition Stats {}", partitionedStats);
        numConsumers =
            partitionedStats
                .getSubscriptions()
                .values()
                .stream()
                .mapToInt(s -> s.getConsumers().size())
                .sum();

      } else {
        TopicStats stats = session.getFactory().getPulsarAdmin().topics().getStats(topic);
        log.info("Stats {}", stats);
        numConsumers =
            stats.getSubscriptions().values().stream().mapToInt(s -> s.getConsumers().size()).sum();
      }

      if (numConsumers > 0) {
        try {
          Thread.sleep(1000);
        } catch (final InterruptedException ex) {
          // Ignore these
        }
      }

    } while (numConsumers > 0 && numAttempts++ < 3);

    return numConsumers;
  }
}
