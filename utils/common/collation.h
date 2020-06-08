// These are the common headers needed to use the MariaDB collation library

// This must be included after any boost headers, or anything that includes
// boost headers. <mariadb.h> and boost are not friends.
#include <mariadb.h>
#undef set_bits  // mariadb.h defines set_bits, which is incompatible with boost
#include <my_sys.h>
#include <m_ctype.h>
