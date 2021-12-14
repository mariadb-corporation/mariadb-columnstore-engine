#pragma once

#include <unistd.h>
#include <string>

namespace qfe
{
void processCreateStmt(const std::string&, uint32_t);
void processDropStmt(const std::string&, uint32_t);
}


