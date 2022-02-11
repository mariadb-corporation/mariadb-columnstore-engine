/* Copyright (C) 2021 MariaDB Corporation

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

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "idbcompress.h"

class CompressionTest : public ::testing::Test
{
 protected:
  std::string genPermutations(string& data)
  {
    std::string generated;
    generate(data, 0, generated);
    return generated;
  }

 private:
  void generate(string& data, uint32_t i, std::string& generated)
  {
    if (i == data.size())
    {
      generated.append(data);
      return;
    }

    for (uint32_t k = i, e = data.size(); k < e; ++k)
    {
      std::swap(data[i], data[k]);
      generate(data, i + 1, generated);
      std::swap(data[i], data[k]);
    }
  }
};

TEST_F(CompressionTest, LZ4CanCompress)
{
  std::string originalData =
      "This program is free software; you can redistribute it and/or"
      "modify it under the terms of the GNU General Public License"
      "as published by the Free Software Foundation; version 2 of"
      "the License.";

  std::unique_ptr<compress::CompressInterface> compressor(new compress::CompressInterfaceLZ4());

  size_t originalSize = originalData.size();
  size_t compressedSize = compressor->maxCompressedSize(originalSize);
  std::unique_ptr<char[]> compressedData(new char[compressedSize]);
  std::memset(compressedData.get(), 0, compressedSize);

  auto rc = compressor->compress(originalData.data(), originalSize, compressedData.get(), &compressedSize);
  ASSERT_EQ(rc, 0);

  std::unique_ptr<char[]> uncompressedData(new char[originalSize]);
  rc = compressor->uncompress(compressedData.get(), compressedSize, uncompressedData.get(), &originalSize);
  ASSERT_EQ(rc, 0);
  std::string result(uncompressedData.get());
  EXPECT_EQ(originalData, result);
}

TEST_F(CompressionTest, LZvsSnappyUnique)
{
  std::unique_ptr<compress::CompressInterface> lz4Compressor(new compress::CompressInterfaceLZ4());
  std::unique_ptr<compress::CompressInterface> snappyCompressor(new compress::CompressInterfaceSnappy());
  // Generate permutations.
  // 9! * 9 == 3265920 (closer to current chunk size)
  std::vector<std::string> dataPool{"abcdefghi", "aaadefghi", "aaaaafghi", "aaaaaaahi", "aaaaaaaaj"};

  for (auto& data : dataPool)
  {
    std::cout << "Permutations generated for: " << data << std::endl;
    auto generated = genPermutations(data);
    auto generatedSize = generated.size();

    auto compressedSizeLZ4 = lz4Compressor->maxCompressedSize(generatedSize);
    auto compressedSizeSnappy = snappyCompressor->maxCompressedSize(generatedSize);

    std::unique_ptr<char[]> lz4CompressedData(new char[compressedSizeLZ4]);
    auto rc =
        lz4Compressor->compress(generated.data(), generatedSize, lz4CompressedData.get(), &compressedSizeLZ4);
    ASSERT_EQ(rc, 0);

    std::unique_ptr<char[]> snappyCompressedData(new char[compressedSizeSnappy]);
    rc = snappyCompressor->compress(generated.data(), generatedSize, snappyCompressedData.get(),
                                    &compressedSizeSnappy);
    ASSERT_EQ(rc, 0);

    std::cout << "LZ ratio: " << (float)((float)generatedSize / (float)compressedSizeLZ4) << std::endl;

    std::cout << "Snappy ratio: " << (float)((float)generatedSize / (float)compressedSizeSnappy) << std::endl;
  }
}
