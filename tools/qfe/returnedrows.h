#pragma once

#include "socktype.h"
#include "messagequeue.h"

namespace qfe
{

void processReturnedRows(messageqcpp::MessageQueueClient*, SockType);

}


