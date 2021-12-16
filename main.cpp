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
#include "rdkafkacpp.h"
#include "json.hpp"

#include <QtCore>

using json = nlohmann::json;

static volatile sig_atomic_t run = 1;
static bool exit_eof             = false;

static void sigterm(int sig) {
  run = 0;
}


static int eof_cnt               = 0;
static int partition_cnt         = 0;
static int verbosity             = 1;
static long msg_cnt              = 0;
static int64_t msg_bytes         = 0;

void msg_consume(RdKafka::Message *message, void *opaque) {
  switch (message->err()) {
  case RdKafka::ERR__TIMED_OUT:
    break;

  case RdKafka::ERR_NO_ERROR:
    /* Real message */
    msg_cnt++;
    msg_bytes += message->len();
    if (verbosity >= 3)
      std::cerr << "Read msg at offset " << message->offset() << std::endl;
    RdKafka::MessageTimestamp ts;
    ts = message->timestamp();
    if (verbosity >= 2 &&
        ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
      std::string tsname = "?";
      if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
        tsname = "create time";
      else if (ts.type ==
               RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
        tsname = "log append time";
      std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
    }
    if (verbosity >= 2 && message->key()) {
      std::cout << "Key: " << *message->key() << std::endl;
    }
    if (verbosity >= 1) {
      printf("%.*s\n", static_cast<int>(message->len()),
             static_cast<const char *>(message->payload()));
    }
    break;

  case RdKafka::ERR__PARTITION_EOF:
    /* Last message */
    if (exit_eof && ++eof_cnt == partition_cnt) {
      std::cerr << "%% EOF reached for all " << partition_cnt << " partition(s)"
                << std::endl;
      run = 0;
    }
    break;

  case RdKafka::ERR__UNKNOWN_TOPIC:
  case RdKafka::ERR__UNKNOWN_PARTITION:
    std::cerr << "Consume failed: " << message->errstr() << std::endl;
    run = 0;
    break;

  default:
    /* Errors */
    std::cerr << "Consume failed: " << message->errstr() << std::endl;
    run = 0;
  }
}


void readJson()
   {
      QString val;
      QFile file;
      file.setFileName("test.json");
      file.open(QIODevice::ReadOnly | QIODevice::Text);
      val = file.readAll();
      file.close();
      qWarning() << val;
      QJsonDocument d = QJsonDocument::fromJson(val.toUtf8());
      QJsonObject sett2 = d.object();
      QJsonValue value = sett2.value(QString("appName"));
      qWarning() << value;
      QJsonObject item = value.toObject();
      qWarning() << ("QJsonObject of description: ") << item;

      /* in case of string value get value and convert into string*/
      qWarning() << ("QJsonObject[appName] of description: ") << item["description"];
      QJsonValue subobj = item["description"];
      qWarning() << subobj.toString();

      /* in case of array get array and convert into string*/
      qWarning() << ("QJsonObject[appName] of value: ") << item["imp"];
      QJsonArray test = item["imp"].toArray();
      qWarning() << test[1].toString();
   }


int main(int argc, char **argv) {
  std::string brokers = "10.0.111.10:9092";
  std::string errstr;
  std::vector<std::string>  topic = {"modem"};
  std::string mode;
  std::string debug;
  int32_t partition    = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;

  RdKafka::Conf *conf  = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);



  /*
   * Set parameters manually
   */

  partition = 0;
//  start_offset = 100;

  conf->set("group.id","0",errstr);
  conf->set("metadata.broker.list", brokers, errstr);

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  conf->set("enable.partition.eof", "true", errstr);


  RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!consumer) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  std::cout << "% Created consumer " << consumer->name() << std::endl;

  RdKafka::ErrorCode err = consumer->subscribe(topic);
  if (err) {
    std::cerr << "Failed to subscribe to " << topic.size()
              << " topics: " << RdKafka::err2str(err) << std::endl;
    exit(1);
  }

  /*
   * Start consumer for topic+partition at start offset
   */
//  RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);

//  if (resp != RdKafka::ERR_NO_ERROR) {
//    std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
//              << std::endl;
//    exit(1);
//  }


  while (run) {
      RdKafka::Message *msg = consumer->consume(1000);
      msg_consume(msg, NULL);

//      const char* json_file = R"(
//            {
//             ...
//          )";


      std::cout<<typeid(*msg).name()<<std::endl;

      std::cout<<msg<<std::endl;
//      std::cout << input_json << "\n";
//      auto json_file = static_cast<const char*>(result);

//      json parsed_json = json::parse(input_json);

//      std::cout<<parsed_json["object"]["bbox"] << "\n";

//      int bottom_right_x = parsed_json["object"]["bbox"]["bottomrightx"];
//      int bottom_right_y = parsed_json["object"]["bbox"]["bottomrighty"];
//      int top_left_x = parsed_json["object"]["bbox"]["topleftx"];
//      int top_left_y = parsed_json["object"]["bbox"]["toplefty"];

//      std::cout<< "top left x:" << top_left_x << "top left y: " << top_left_y << "\n";
//      std::cout<< "bottom right x:" << bottom_right_x << "bottom right y: " << bottom_right_y<< "\n";

      delete msg;
      consumer->poll(0);
  }

  consumer->close();
//  consumer->poll(1000);

//  delete topic;
  delete consumer;
  delete conf;
  delete tconf;


  RdKafka::wait_destroyed(5000);

  return 0;
}
