#include <utils/json/json.hpp>
#include <utils/loggingcpp/logger.h>
#include <utils/messageqcpp/serializeable.h>

#include "pron.h"

namespace utils
{

void makeLog(logging::Message::Args& args)
{
  logging::Message msg(1);
  msg.format(args);
  logging::LoggingID logid(20, 0, 0);
  logging::Logger logger(logid.fSubsysID);
  logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);
}

void Pron::pron(std::string& pron)
{
  if (pron.empty())
  {
    pron_.clear();
    return;
  }

  try
  {
    nlohmann::json j = nlohmann::json::parse(pron);
    pron_ = j.get<StringMap>();

    logging::Message::Args args;
    args.add("Pron settings were set: ");
    args.add(pron);
    makeLog(args);
  }
  catch (const std::exception& e)
  {
    logging::Message::Args args;
    args.add("Pron parsing error: ");
    args.add(e.what());
    makeLog(args);
  }
}

}  // namespace utils