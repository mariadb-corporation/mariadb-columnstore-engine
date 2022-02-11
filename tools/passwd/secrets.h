/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2025-05-25
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#include <vector>
#include <string>
#include "idberrorinfo.h"
#include "logger.h"

class CSPasswdLogging
{
 public:
  static CSPasswdLogging* get();
  ~CSPasswdLogging();

  void log(int priority, const char* format, ...);

 private:
  CSPasswdLogging();
};

/**
 * Decrypt an encrypted password using the key loaded at startup. If the password is not encrypted,
 * ie is not a HEX string, return the original.
 *
 * @param input The encrypted password
 * @return The decrypted password.
 */
std::string decrypt_password(const std::string& input);

#include <memory>

using ByteVec = std::vector<uint8_t>;

struct evp_cipher_st;
extern const char* const SECRETS_FILENAME;

/**
 * Returns the cipher used for password encryption.
 *
 * @return Cipher
 */
const evp_cipher_st* secrets_cipher();

/**
 * Returns encryption key length.
 *
 * @return Encryption key length
 */
int secrets_keylen();

/**
 * Returns initialization vector length.
 *
 * @return initialization vector length
 */
int secrets_ivlen();

bool load_encryption_keys();
std::string encrypt_password_old(const ByteVec& key, const ByteVec& iv, const std::string& input);
std::string encrypt_password(const ByteVec& key, const std::string& input);

std::string decrypt_password_old(const ByteVec& key, const ByteVec& iv, const std::string& input);
std::string decrypt_password(const ByteVec& key, const std::string& input);

struct ReadKeyResult
{
  bool ok{false};
  ByteVec key;
  ByteVec iv;
};

ReadKeyResult secrets_readkeys(const std::string& filepath);
bool secrets_write_keys(const ByteVec& key, const std::string& filepath, const std::string& owner);
