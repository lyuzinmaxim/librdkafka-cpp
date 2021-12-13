#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#ifdef _WIN64
#include "C:/librdkafka-master/win32/wingetopt.h"
#elif _AIX
#include <unistd.h>
#include wingetopt.c
#else
#include <getopt.h>
#endif

/*
 * Typically include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include "rdkafkacpp.h"


static char metadata_print(const std::string &topic,
                           const RdKafka::Metadata *metadata) {
  std::cout << "Metadata for " << (topic.empty() ? "" : "all topics")
            << "(from broker " << metadata->orig_broker_id() << ":"
            << metadata->orig_broker_name() << std::endl;

  /* Iterate brokers */
  std::cout << " " << metadata->brokers()->size() << " brokers:" << std::endl;
  RdKafka::Metadata::BrokerMetadataIterator ib;
  for (ib = metadata->brokers()->begin(); ib != metadata->brokers()->end();
       ++ib) {
    std::cout << "  broker " << (*ib)->id() << " at " << (*ib)->host() << ":"
              << (*ib)->port() << std::endl;
  }
  /* Iterate topics */
  std::cout << metadata->topics()->size() << " topics:" << std::endl;
  RdKafka::Metadata::TopicMetadataIterator it;
  for (it = metadata->topics()->begin(); it != metadata->topics()->end();
       ++it) {
    std::cout << "  topic \"" << (*it)->topic() << "\" with "
              << (*it)->partitions()->size() << " partitions:";

    if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
      std::cout << " " << err2str((*it)->err());
      if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE)
        std::cout << " (try again)";
    }
    std::cout << std::endl;

    /* Iterate topic's partitions */
    RdKafka::TopicMetadata::PartitionMetadataIterator ip;
    for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end();
         ++ip) {
      std::cout << "    partition " << (*ip)->id() << ", leader "
                << (*ip)->leader() << ", replicas: ";

      /* Iterate partition's replicas */
      RdKafka::PartitionMetadata::ReplicasIterator ir;
      for (ir = (*ip)->replicas()->begin(); ir != (*ip)->replicas()->end();
           ++ir) {
        std::cout << (ir == (*ip)->replicas()->begin() ? "" : ",") << *ir;
      }

      /* Iterate partition's ISRs */
      std::cout << ", isrs: ";
      RdKafka::PartitionMetadata::ISRSIterator iis;
      for (iis = (*ip)->isrs()->begin(); iis != (*ip)->isrs()->end(); ++iis)
        std::cout << (iis == (*ip)->isrs()->begin() ? "" : ",") << *iis;

      if ((*ip)->err() != RdKafka::ERR_NO_ERROR)
        std::cout << ", " << RdKafka::err2str((*ip)->err()) << std::endl;
      else
        std::cout << std::endl;
    }
  }
}

static volatile sig_atomic_t run = 1;
static bool exit_eof             = false;

static void sigterm(int sig) {
  run = 0;
}


class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    std::string status_name;
    switch (message.status()) {
    case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
      status_name = "NotPersisted";
      break;
    case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
      status_name = "PossiblyPersisted";
      break;
    case RdKafka::Message::MSG_STATUS_PERSISTED:
      status_name = "Persisted";
      break;
    default:
      status_name = "Unknown?";
      break;
    }
    std::cout << "Message delivery for (" << message.len()
              << " bytes): " << status_name << ": " << message.errstr()
              << std::endl;
    if (message.key())
      std::cout << "Key: " << *(message.key()) << ";" << std::endl;
  }
};


class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      if (event.fatal()) {
        std::cerr << "FATAL ";
        run = 0;
      }
      std::cerr << "ERROR (" << RdKafka::err2str(event.err())
                << "): " << event.str() << std::endl;
      break;

    case RdKafka::Event::EVENT_STATS:
      std::cerr << "\"STATS\": " << event.str() << std::endl;
      break;

    case RdKafka::Event::EVENT_LOG:
      fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(),
              event.str().c_str());
      break;

    default:
      std::cerr << "EVENT " << event.type() << " ("
                << RdKafka::err2str(event.err()) << "): " << event.str()
                << std::endl;
      break;
    }
  }
};


/* Use of this partitioner is pretty pointless since no key is provided
 * in the produce() call. */
class MyHashPartitionerCb : public RdKafka::PartitionerCb {
 public:
  int32_t partitioner_cb(const RdKafka::Topic *topic,
                         const std::string *key,
                         int32_t partition_cnt,
                         void *msg_opaque) {
    return djb_hash(key->c_str(), key->size()) % partition_cnt;
  }

 private:
  static inline unsigned int djb_hash(const char *str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++)
      hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
};

//void msg_consume(RdKafka::Message *message, void *opaque) {
//  const RdKafka::Headers *headers;

//  switch (message->err()) {
//  case RdKafka::ERR__TIMED_OUT:
//    break;

//  case RdKafka::ERR_NO_ERROR:
//    /* Real message */
//    std::cout << "Read msg at offset " << message->offset() << std::endl;
//    if (message->key()) {
//      std::cout << "Key: " << *message->key() << std::endl;
//    }
//    headers = message->headers();
//    if (headers) {
//      std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
//      for (size_t i = 0; i < hdrs.size(); i++) {
//        const RdKafka::Headers::Header hdr = hdrs[i];

//        if (hdr.value() != NULL)
//          printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
//                 (int)hdr.value_size(), (const char *)hdr.value());
//        else
//          printf(" Header:  %s = NULL\n", hdr.key().c_str());
//      }
//    }
//    printf("%.*s\n", static_cast<int>(message->len()),
//           static_cast<const char *>(message->payload()));
//    break;

//  case RdKafka::ERR__PARTITION_EOF:
//    /* Last message */
//    if (exit_eof) {
//      run = 0;
//    }
//    break;

//  case RdKafka::ERR__UNKNOWN_TOPIC:
//  case RdKafka::ERR__UNKNOWN_PARTITION:
//    std::cerr << "Consume failed: " << message->errstr() << std::endl;
//    run = 0;
//    break;

//  default:
//    /* Errors */
//    std::cerr << "Consume failed: " << message->errstr() << std::endl;
//    run = 0;
//  }
//}


auto msg_consume(RdKafka::Message *message, void *opaque) {
  const RdKafka::Headers *headers;

  if (message->err()==RdKafka::ERR_NO_ERROR) {
//    std::cout << "Read msg at offset " << message->offset() << std::endl;
//    if (message->key()) {
//      std::cout << "Key: " << *message->key() << std::endl;
//    }
    headers = message->headers();
    if (headers) {
      std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
      for (size_t i = 0; i < hdrs.size(); i++) {
        const RdKafka::Headers::Header hdr = hdrs[i];

        if (hdr.value() != NULL)
          printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
                 (int)hdr.value_size(), (const char *)hdr.value());
        else
          printf(" Header:  %s = NULL\n", hdr.key().c_str());
      }
    }
    auto input_msg = static_cast<const char *> (message->payload());
//    std::cout<<input_msg;
    return input_msg;
  } else if (message->err()==RdKafka::ERR__PARTITION_EOF) {
      /* Last message */
          if (exit_eof) {
            run = 0;
          };
//          break;
  } else {
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
          run = 0;
  }
};


class ExampleConsumeCb : public RdKafka::ConsumeCb {
 public:
  void consume_cb(RdKafka::Message &msg, void *opaque) {
    msg_consume(&msg, opaque);
  }
};



int main(int argc, char **argv) {
  std::string brokers = "10.0.111.10:9092";
  std::string errstr;
  std::string topic_str = "modem";
  std::string mode;
  std::string debug;
  int32_t partition    = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
  bool do_conf_dump    = false;
//  int opt;
  MyHashPartitionerCb hash_partitioner;
  int use_ccb = 0;

  /*
   * Create configuration objects
   */
  RdKafka::Conf *conf  = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  /*
   * Set parameters manually
   */
  partition = 0;

  conf->set("metadata.broker.list", brokers, errstr);

  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }
  }

  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);

  if (do_conf_dump) {
    int pass;

    for (pass = 0; pass < 2; pass++) {
      std::list<std::string> *dump;
      if (pass == 0) {
        dump = conf->dump();
        std::cout << "# Global config" << std::endl;
      } else {
        dump = tconf->dump();
        std::cout << "# Topic config" << std::endl;
      }

      for (std::list<std::string>::iterator it = dump->begin();
           it != dump->end();) {
        std::cout << *it << " = ";
        it++;
        std::cout << *it << std::endl;
        it++;
      }
      std::cout << std::endl;
    }
    exit(0);
  }

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  conf->set("enable.partition.eof", "true", errstr);

  RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
  if (!consumer) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  std::cout << "% Created consumer " << consumer->name() << std::endl;

  RdKafka::Topic *topic =
      RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }

  /*
   * Start consumer for topic+partition at start offset
   */
  RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
//  RdKafka::ErrorCode resp = consumer->start(topic, partition, 60);
  if (resp != RdKafka::ERR_NO_ERROR) {
    std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
              << std::endl;
    exit(1);
  }

  ExampleConsumeCb ex_consume_cb;

  /*
   * Consume messages
   */
  while (run) {
    if (use_ccb) {
      consumer->consume_callback(topic, partition, 1000, &ex_consume_cb,
                                 &use_ccb);
    } else {
      RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
      auto result = msg_consume(msg, NULL);
      std::cout<<result<<"\n";
      delete msg;
    }
    consumer->poll(0);
  }

  /*
   * Stop consumer
   */
  consumer->stop(topic, partition);

  consumer->poll(1000);

  delete topic;
  delete consumer;
  delete conf;
  delete tconf;

  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (when check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
//  RdKafka::wait_destroyed(5000);

  return 0;
}
