#pragma once

#include <unistd.h>
#include <string>

#include "calpontselectexecutionplan.h"

namespace qfe
{

execplan::CalpontSelectExecutionPlan* parseQuery(const std::string&, uint32_t);

}

