/* Copyright (C) 2024 MariaDB Corporation, Inc.

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

/*
 * A tool that can take a csv file of extent map entires and produce a BRM_saves_em-compatible file.
 * If you re-compile extentmap.cpp to dump the extent map as it loads, you'll get a csv file on stdout.
 * Save this to a file and edit it as needed (remove the cruft at the top & bottom for sure). Then use
 * this tool to create a binary BRM_saves_em file.
 */

#include <iostream>
#include <stdint.h>
#include <fstream>
#include <cerrno>
#include <string>
#include <cstdlib>
#include <cassert>
#include <limits>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

using namespace std;

#include "extentmap.h"

template <typename T>
T parseField(std::stringstream& ss, const char delimiter)
{
  std::string field;
  std::getline(ss, field, delimiter);
  return std::stoll(field);
}

BRM::EMEntry parseLine(const std::string& line, char delimiter = '|')
{
  std::stringstream ss(line);
  std::string field;

  auto rangeStart = parseField<int64_t>(ss, delimiter);
  auto rangeSize = parseField<uint32_t>(ss, delimiter);
  BRM::InlineLBIDRange range{rangeStart, rangeSize};

  auto fileID = parseField<int>(ss, delimiter);
  auto blockOffset = parseField<uint32_t>(ss, delimiter);
  auto hwm = parseField<BRM::HWM_t>(ss, delimiter);
  auto partitionNum = parseField<BRM::PartitionNumberT>(ss, delimiter);
  auto segmentNum = parseField<uint16_t>(ss, delimiter);
  auto dbRoot = parseField<BRM::DBRootT>(ss, delimiter);
  auto colWid = parseField<uint16_t>(ss, delimiter);
  auto status = parseField<int16_t>(ss, delimiter);

  auto hiVal = parseField<int64_t>(ss, delimiter);
  auto loVal = parseField<int64_t>(ss, delimiter);
  auto sequenceNum = parseField<int32_t>(ss, delimiter);
  auto isValid = parseField<char>(ss, delimiter);
  auto partition = BRM::EMCasualPartition_t{loVal, hiVal, sequenceNum, isValid};

  return BRM::EMEntry{range,      fileID, blockOffset, hwm,    partitionNum,
                      segmentNum, dbRoot, colWid,      status, {partition}};
}

int main(int argc, char** argv)
{
  po::options_description desc(
      "A tool to build Extent Map image file from its text representation. A text representation can be "
      "obtained using 'editem -i'"
      "display the lock state.");

  std::string srcFilename;
  std::string dstFilename;
  bool debug = false;

  // clang-format off
  desc.add_options()
    ("help", "produce help message")
    ("input-filename,i",
        po::value<std::string>(&srcFilename)->required(),
        "Extent Map as its text representation.")
    ("output-filename,o",
        po::value<std::string>(&dstFilename)->default_value(""),
        "Extent Map output image file, default as input-filename.out")
    ("debug,d", po::bool_switch(&debug)->default_value(false), "Print extra output");
  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);

  if (argc == 1 || vm.count("help"))
  {
    cout << desc << "\n";
    return 1;
  }

  po::notify(vm);

  ifstream in(srcFilename);
  int e = errno;

  if (!in)
  {
    cerr << "file read error: " << strerror(e) << endl;
    return 1;
  }

  // Brute force count the number of lines
  int numEMEntries = 0;
  {
    string line;
    getline(in, line);
    while (!in.eof())
    {
      numEMEntries++;
      getline(in, line);
    }
  }

  std::cout << "Number of entries: " << numEMEntries << std::endl;

  if (dstFilename.empty())
  {
    dstFilename = srcFilename + ".out";
  }
  ofstream outFile(dstFilename);

  BRM::InlineLBIDRange maxLBIDinUse{0, 0};

  std::ifstream infile(srcFilename);
  if (!infile.is_open())
  {
    std::cerr << "Can not open file " << srcFilename << std::endl;
    return 1;
  }

  int loadSize[3];
  loadSize[0] = EM_MAGIC_V5;
  loadSize[1] = numEMEntries;
  loadSize[2] = 1;  // one free list entry
  outFile.write((char*)&loadSize, (3 * sizeof(int)));

  string line;
  while (std::getline(infile, line))
  {
    BRM::EMEntry em = parseLine(line);

    if (em.range.start > maxLBIDinUse.start)
    {
      maxLBIDinUse.start = em.range.start;
      maxLBIDinUse.size = em.range.size;
    }

    if (debug)
    {
      std::cout << em.range.start << '\t' << em.range.size << '\t' << em.fileID << '\t' << em.blockOffset
                << '\t' << em.HWM << '\t' << em.partitionNum << '\t' << em.segmentNum << '\t' << em.dbRoot
                << '\t' << em.colWid << '\t' << em.status << '\t' << em.partition.cprange.hiVal << '\t'
                << em.partition.cprange.loVal << '\t' << em.partition.cprange.sequenceNum << '\t'
                << (short int)(em.partition.cprange.isValid) << std::endl;
    }

    outFile.write((char*)&em, sizeof(em));
  }
  infile.close();

  auto flStart = maxLBIDinUse.start + maxLBIDinUse.size * 1024;
  assert(flStart / 1024 <= numeric_limits<uint32_t>::max());
  uint32_t flEnd = numeric_limits<uint32_t>::max() - flStart / 1024;
  BRM::InlineLBIDRange fl{flStart, flEnd};

  outFile.write((char*)&fl, sizeof(fl));
  outFile.close();

  return 0;
}
