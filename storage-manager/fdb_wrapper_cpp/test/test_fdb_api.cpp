/* Copyright (C) 2024 MariaDB Corporation

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

#include "../fdb_wrapper_cpp/include/fdbcs.hpp"

using namespace std;
using namespace FDBCS;

template <typename T>
void assert_internal(const T& value)
{
  if (!value)
    abort();
}

int main()
{
  std::string path = "/etc/foundationdb/fdb.cluster";
  assert_internal(setAPIVersion());
  FDBNetwork netWork;
  // Create and run network.
  assert_internal(netWork.setUpAndRunNetwork());

  // Create database.
  std::unique_ptr<FDBDataBase> db = DataBaseCreator::createDataBase(path);
  assert_internal(db);
  assert_internal(db->isDataBaseReady());

  std::string key1 = "fajsdlkfjaskljfewiower39423fds";
  std::string value1 = "gfdgjksdflfdsjkslkdrewuior39243";
  // Set a key/value.
  {
    auto tnx = db->createTransaction();
    tnx->set(key1, value1);
    assert_internal(tnx->commit());
  }
  // Get a value by a key.
  {
    auto tnx = db->createTransaction();
    auto p = tnx->get(key1);
    assert_internal(p.first);
    assert_internal(p.second == value1);
  }
  // Remove a key.
  {
    auto tnx = db->createTransaction();
    tnx->remove(key1);
    assert_internal(tnx->commit());
  }
  // Check that key is not presetnt anymore.
  {
    auto tnx = db->createTransaction();
    auto p = tnx->get(key1);
    assert_internal(!p.first);
  }

  return 0;
}
