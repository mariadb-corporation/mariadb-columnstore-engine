/* Copyright (C) 2016-2025 MariaDB Corporation

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
#include <iostream>

#include "dbrm.h"

#include <boost/program_options.hpp>
namespace po = boost::program_options;

template <class T>
void check_value(const boost::program_options::variables_map& vm, const std::string& opt1, T lower_bound,
                 T upper_bound)
{
  auto value = vm[opt1].as<T>();
  if (value < lower_bound || value >= upper_bound)
  {
    throw std::logic_error(std::string("Option '") + opt1 + "' is out of range.:  " + std::to_string(value));
  }
}

int main(int argc, char** argv)
{
  po::options_description desc(
      "A tool to reclaim OID space. It needs controllernode to be online to operate ");

  uint32_t lowerOid;
  uint32_t upperOid;
  desc.add_options()("help", "produce help message")(
      "lower-oid,l", po::value<uint32_t>(&lowerOid)->required(), "lower oid that can not be lower 3000")(
      "upper-oid,u", po::value<uint32_t>(&upperOid)->required(),
      "lower oid that can not be bigger 16 * 1024 ^ 2");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);

  if (argc == 1 || vm.count("help"))
  {
    std::cout << desc << "\n";
    return 1;
  }

  uint32_t bitMapSize = 8 * 2 * 1024 * 1024;
  check_value<int>(vm, "lower-oid", 3000, bitMapSize - 1);
  check_value<int>(vm, "upper-oid", lowerOid, bitMapSize);

  po::notify(vm);

  BRM::DBRM brm;
  std::cout << "OID space size before reclaim: " << brm.oidm_size() << std::endl;
  brm.returnOIDs(lowerOid, upperOid);
  std::cout << "OID space size after reclaim:" << brm.oidm_size() << std::endl;
  return 0;
}