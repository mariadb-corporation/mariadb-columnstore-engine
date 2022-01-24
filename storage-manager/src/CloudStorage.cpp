/* Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "CloudStorage.h"
#include "Config.h"
#include "S3Storage.h"
#include "LocalStorage.h"
#include "SMLogging.h"
#include <boost/thread/mutex.hpp>
#include <string>
#include <ctype.h>
#include <iostream>

using namespace std;

namespace
{
boost::mutex m;
storagemanager::CloudStorage* inst = NULL;

string tolower(const string& s)
{
  string ret(s);
  for (uint i = 0; i < ret.length(); i++)
    ret[i] = ::tolower(ret[i]);
  return ret;
}

}  // namespace

namespace storagemanager
{
CloudStorage* CloudStorage::get()
{
  if (inst)
    return inst;

  SMLogging* logger = SMLogging::get();
  Config* conf = Config::get();
  string type = tolower(conf->getValue("ObjectStorage", "service"));
  boost::mutex::scoped_lock s(m);
  if (inst)
    return inst;
  if (type == "s3")
    inst = new S3Storage();
  else if (type == "local" || type == "localstorage")
    inst = new LocalStorage();
  else
  {
    logger->log(LOG_CRIT, "CloudStorage: got unknown service provider: %s", type.c_str());
    throw runtime_error("CloudStorage: got unknown service provider");
  }

  return inst;
}

CloudStorage::CloudStorage()
{
  logger = SMLogging::get();

  bytesUploaded = bytesDownloaded = objectsDeleted = objectsCopied = objectsGotten = objectsPut =
      existenceChecks = 0;
}

void CloudStorage::printKPIs() const
{
  cout << "CloudStorage" << endl;
  cout << "\tbytesUplaoded = " << bytesUploaded << endl;
  cout << "\tbytesDownloaded = " << bytesDownloaded << endl;
  cout << "\tobjectsDeleted = " << objectsDeleted << endl;
  cout << "\tobjectsCopied = " << objectsCopied << endl;
  cout << "\tobjectsGotten = " << objectsGotten << endl;
  cout << "\tobjectsPut = " << objectsPut << endl;
  cout << "\texistenceChecks = " << existenceChecks << endl;
}

}  // namespace storagemanager
