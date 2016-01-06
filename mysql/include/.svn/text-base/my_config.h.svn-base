#ifndef MYSQL_MY_CONFIG_H__
#define MYSQL_MY_CONFIG_H__

#if __linux__
#if __LP64__
#include "my_config-linux64.h"
#else //!__LP64__
#include "my_config-linux32.h"
#endif //__LP64__

#elif __FreeBSD__
#if __LP64__
#include "my_config-freebsd64.h"
#else //!__LP64__
#error No port for this os!
#endif //__LP64__

#else
#error No port for this os!
#endif

#endif //!MYSQL_MY_CONFIG_H__

