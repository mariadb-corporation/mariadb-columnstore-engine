#pragma once

#include "messagequeue.h"
#include "calpontselectexecutionplan.h"

namespace qfe
{

//Takes ownership of the alloc'd ptr
//Returns an alloc'd mqc
messageqcpp::MessageQueueClient* sendCSEP(execplan::CalpontSelectExecutionPlan*);

}


