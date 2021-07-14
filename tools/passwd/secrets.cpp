/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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
#include "secrets.h"

#include <cctype>
#include <fstream>
#include <pwd.h>
#include <sys/stat.h>
#include <openssl/aes.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/rand.h>

#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "idberrorinfo.h"
#include "logger.h"
#include "mcsconfig.h"
#include "exceptclasses.h"
#include "columnstoreversion.h"

using std::string;

const char* const SECRETS_FILENAME = ".secrets";


namespace
{
CSPasswdLogging *passwdLog = NULL;
boost::mutex m;
}

CSPasswdLogging::CSPasswdLogging()
{
    //TODO: make this configurable
    setlogmask (LOG_UPTO (LOG_DEBUG));
    openlog ("CSPasswd", LOG_PID | LOG_NDELAY | LOG_PERROR | LOG_CONS, LOG_LOCAL1);
}

CSPasswdLogging::~CSPasswdLogging()
{
    syslog(LOG_INFO, "CloseLog");
    closelog();
}

CSPasswdLogging * CSPasswdLogging::get()
{
    if (passwdLog)
        return passwdLog;
    boost::mutex::scoped_lock s(m);
    if (passwdLog)
        return passwdLog;
    passwdLog = new CSPasswdLogging();
    return passwdLog;
}

void CSPasswdLogging::log(int priority,const char *format, ...)
{
    va_list args;
    va_start(args, format);

    #ifdef DEBUG
    va_list args2;
    va_copy(args2, args);
    vfprintf(stderr, format, args2);
    fprintf(stderr, "\n");
    va_end(args2);
    #endif
    vsyslog(priority, format, args);

    va_end(args);
}

namespace
{

using HexLookupTable = std::array<uint8_t, 256>;
HexLookupTable init_hex_lookup_table() noexcept;

// Hex char -> byte val lookup table.
const HexLookupTable hex_lookup_table = init_hex_lookup_table();

/* used in the bin2hex function */
const char hex_upper[] = "0123456789ABCDEF";
const char hex_lower[] = "0123456789abcdef";

HexLookupTable init_hex_lookup_table() noexcept
{
    auto char_val = [](char c) -> uint8_t {
            if (c >= '0' && c <= '9')
            {
                return c - '0';
            }
            else if (c >= 'A' && c <= 'F')
            {
                return c - 'A' + 10;
            }
            else if (c >= 'a' && c <= 'f')
            {
                return c - 'a' + 10;
            }
            else
            {
                return '\177';
            }
        };

    HexLookupTable rval;
    for (size_t i = 0; i < rval.size(); i++)
    {
        rval[i] = char_val(i);
    }
    return rval;
}

bool hex2bin(const char* in, unsigned int in_len, uint8_t* out)
{
    // Input length must be multiple of two.
    if (!in || in_len == 0 || in_len % 2 != 0)
    {
        return false;
    }

    const char* in_end = in + in_len;
    while (in < in_end)
    {
        // One byte is formed from two hex chars, with the first char forming the high bits.
        uint8_t high_half = hex_lookup_table[*in++];
        uint8_t low_half = hex_lookup_table[*in++];
        uint8_t total = (high_half << 4) | low_half;
        *out++ = total;
    }
    return true;
}

char* bin2hex(const uint8_t* in, unsigned int len, char* out)
{
    const uint8_t* in_end = in + len;
    if (len == 0 || in == NULL)
    {
        return NULL;
    }

    for (; in != in_end; ++in)
    {
        *out++ = hex_upper[((uint8_t) * in) >> 4];
        *out++ = hex_upper[((uint8_t) * in) & 0x0F];
    }
    *out = '\0';

    return out;
}

struct ThisUnit
{
    ByteVec key;    /**< Password decryption key, assigned at startup */
    ByteVec iv;     /**< Decryption init vector, assigned at startup. Only used with old-format keys */
};
ThisUnit this_unit;

enum class ProcessingMode
{
    ENCRYPT,
    DECRYPT,
    DECRYPT_IGNORE_ERRORS
};

const char field_desc[] = "description";
const char field_version[] = "columnstore_version";
const char field_cipher[] = "encryption_cipher";
const char field_key[] = "encryption_key";
const char desc[] = "Columnstore encryption/decryption key";

#define SECRETS_CIPHER EVP_aes_256_cbc
#define STRINGIFY(X)  #X
#define STRINGIFY2(X) STRINGIFY(X)
const char CIPHER_NAME[] = STRINGIFY2(SECRETS_CIPHER);

void print_openSSL_errors(const char* operation);

/**
 * Encrypt or decrypt the input buffer to output buffer.
 *
 * @param key Encryption key
 * @param mode Encrypting or decrypting
 * @param input Input buffer
 * @param input_len Input length
 * @param output Output buffer
 * @param output_len Produced output length is written here
 * @return True on success
 */
bool encrypt_or_decrypt(const uint8_t* key, const uint8_t* iv, ProcessingMode mode,
                        const uint8_t* input, int input_len, uint8_t* output, int* output_len)
{
    auto ctx = EVP_CIPHER_CTX_new();
    int enc = (mode == ProcessingMode::ENCRYPT) ? AES_ENCRYPT : AES_DECRYPT;
    bool ignore_errors = (mode == ProcessingMode::DECRYPT_IGNORE_ERRORS);
    bool ok = false;

    if (EVP_CipherInit_ex(ctx, secrets_cipher(), nullptr, key, iv, enc) == 1 || ignore_errors)
    {
        int output_written = 0;
        if (EVP_CipherUpdate(ctx, output, &output_written, input, input_len) == 1 || ignore_errors)
        {
            int total_output_len = output_written;
            if (EVP_CipherFinal_ex(ctx, output + total_output_len, &output_written) == 1 || ignore_errors)
            {
                total_output_len += output_written;
                *output_len = total_output_len;
                ok = true;
            }
        }
    }

    EVP_CIPHER_CTX_free(ctx);
    if (!ok)
    {
        const char* operation = (mode == ProcessingMode::ENCRYPT) ? "when encrypting password" :
            "when decrypting password";
        print_openSSL_errors(operation);
    }
    return ok;
}

void print_openSSL_errors(const char* operation)
{
    // It's unclear how thread(unsafe) OpenSSL error functions are. Minimize such possibilities by
    // using a local buffer.
    constexpr size_t bufsize = 256;     // Should be enough according to some googling.
    char buf[bufsize];
    buf[0] = '\0';

    auto errornum = ERR_get_error();
    auto errornum2 = ERR_get_error();
    ERR_error_string_n(errornum, buf, bufsize);

    if (errornum2 == 0)
    {
        // One error.
        CSPasswdLogging::get()->log(LOG_ERR,"OpenSSL error %s. %s", operation, buf);
    }
    else
    {
        // Multiple errors, print all as separate messages.
        CSPasswdLogging::get()->log(LOG_ERR,"Multiple OpenSSL errors %s. Detailed messages below.", operation);
        CSPasswdLogging::get()->log(LOG_ERR,"%s", buf);
        while (errornum2 != 0)
        {
            ERR_error_string_n(errornum2, buf, bufsize);
            CSPasswdLogging::get()->log(LOG_ERR,"%s", buf);
            errornum2 = ERR_get_error();
        }
    }
}
}

const EVP_CIPHER* secrets_cipher()
{
    return SECRETS_CIPHER();
}

int secrets_keylen()
{
    return EVP_CIPHER_key_length(secrets_cipher());
}

int secrets_ivlen()
{
    return EVP_CIPHER_iv_length(secrets_cipher());
}

/**
 * Reads binary or text data from file and extracts the encryption key and, if old key format is used,
 * the init vector. The source file needs to have expected permissions. If the source file does not exist,
 * returns empty results.
 *
 * @param filepath Path to key file.
 * @return Result structure. Ok if file was loaded successfully or if file did not exist. False on error.
 */
ReadKeyResult secrets_readkeys(const string& filepath)
{
    ReadKeyResult rval;
    auto filepathc = filepath.c_str();

    const int binary_key_len = secrets_keylen();
    const int binary_iv_len = secrets_ivlen();
    const int binary_total_len = binary_key_len + binary_iv_len;

    // Before opening the file, check its size and permissions.
    struct stat filestats { 0 };
    bool stat_error = false;
    bool old_format = false;
    errno = 0;
    if (stat(filepathc, &filestats) == 0)
    {
        auto filesize = filestats.st_size;
        if (filesize == binary_total_len)
        {
            old_format = true;
            CSPasswdLogging::get()->log(LOG_WARNING,"File format of '%s' is deprecated. Please generate a new encryption key ('maxkeys') "
                        "and re-encrypt passwords ('maxpasswd').", filepathc);
        }

        auto filemode = filestats.st_mode;
        if (!S_ISREG(filemode))
        {
            CSPasswdLogging::get()->log(LOG_ERR,"Secrets file '%s' is not a regular file.", filepathc);
            stat_error = true;
        }
        else if ((filemode & (S_IRWXU | S_IRWXG | S_IRWXO)) != S_IRUSR)
        {
            CSPasswdLogging::get()->log(LOG_ERR,"Secrets file '%s' permissions are wrong. The only permission on the file should be "
                      "owner:read.", filepathc);
            stat_error = true;
        }
    }
    else if (errno == ENOENT)
    {
        // The file does not exist. This is ok. Return empty result.
        rval.ok = true;
        return rval;
    }
    else
    {
        CSPasswdLogging::get()->log(LOG_ERR,"stat() for secrets file '%s' failed. Error %d, %s.",
                  filepathc, errno, strerror(errno));
        stat_error = true;
    }

    if (stat_error)
    {
        return rval;
    }

    if (old_format)
    {
        errno = 0;
        std::ifstream file(filepath, std::ios_base::binary);
        if (file.is_open())
        {
            // Read all data from file.
            char readbuf[binary_total_len];
            file.read(readbuf, binary_total_len);
            if (file.good())
            {
                // Success, copy contents to key structure.
                rval.key.assign(readbuf, readbuf + binary_key_len);
                rval.iv.assign(readbuf + binary_key_len, readbuf + binary_total_len);
                rval.ok = true;
            }
            else
            {
                CSPasswdLogging::get()->log(LOG_ERR,"Read from secrets file %s failed. Read %li, expected %i bytes. Error %d, %s.",
                          filepathc, file.gcount(), binary_total_len, errno, strerror(errno));
            }
        }
        else
        {
            CSPasswdLogging::get()->log(LOG_ERR,"Could not open secrets file '%s'. Error %d, %s.",
                      filepathc, errno, strerror(errno));
        }
    }
    else
    {
        // File contents should be json.
        //json_error_t err;
        boost::property_tree::ptree jsontree;
        try
        {
            boost::property_tree::read_json(filepath, jsontree);
        }
        catch(boost::property_tree::json_parser::json_parser_error &je)
        {
            std::cout << "Error reading JSON from secrets file: " << je.filename() << " on line: " << je.line() << std::endl;
            std::cout << je.message() << std::endl;
        }
        catch(...)
        {
            printf("Error reading JSON from secrets file '%s' failed. Error %d, %s.\n",
                   filepathc, errno, strerror(errno));
        }
        //json_t* obj = json_load_file(filepathc, 0, &err);
        string enc_cipher = jsontree.get<string>(field_cipher);
        string enc_key = jsontree.get<string>(field_key);
        //const char* enc_cipher = json_string_value(json_object_get(obj, field_cipher));
        //const char* enc_key = json_string_value(json_object_get(obj, field_key));
        bool cipher_ok = !enc_cipher.empty() && (enc_cipher == CIPHER_NAME);
        if (cipher_ok && !enc_key.empty())
        {
            int read_hex_key_len = enc_key.length();
            int expected_hex_key_len = 2 * binary_key_len;
            if (read_hex_key_len == expected_hex_key_len)
            {
                rval.key.resize(binary_key_len);
                hex2bin(enc_key.c_str(), read_hex_key_len, rval.key.data());
                rval.ok = true;
            }
            else
            {
                CSPasswdLogging::get()->log(LOG_ERR,"Wrong encryption key length in secrets file '%s': found %i, expected %i.",
                          filepathc, read_hex_key_len, expected_hex_key_len);
            }
        }
        else
        {
            CSPasswdLogging::get()->log(LOG_ERR,"Secrets file '%s' does not contain expected string fields '%s' and '%s', "
                      "or '%s' is not '%s'.",
                      filepathc, field_cipher, field_key, field_cipher, CIPHER_NAME);
        }
        jsontree.clear();
    }
    return rval;
}

string decrypt_password(const string& input)
{
    const auto& key = this_unit.key;
    string rval;
    if (key.empty())
    {
        // Password encryption is not used, return original.
        rval = input;
    }
    else
    {
        // If the input is not a HEX string, return the input as is.
        auto is_hex = std::all_of(input.begin(), input.end(), isxdigit);
        if (is_hex)
        {
            const auto& iv = this_unit.iv;
            rval = iv.empty() ? ::decrypt_password(key, input) : decrypt_password_old(key, iv, input);
        }
        else
        {
            rval = input;
        }
    }
    return rval;
}

/**
 * Decrypt passwords encrypted with an old (pre 2.5) .secrets-file. The decryption also depends on whether
 * the password was encrypted using maxpasswd 2.4 or 2.5.
 *
 * @param key Encryption key
 * @param iv Init vector
 * @param input Encrypted password in hex form
 * @return Decrypted password or empty on error
 */
string decrypt_password_old(const ByteVec& key, const ByteVec& iv, const std::string& input)
{
    string rval;
    // Convert to binary.
    size_t hex_len = input.length();
    auto bin_len = hex_len / 2;
    unsigned char encrypted_bin[bin_len];
    hex2bin(input.c_str(), hex_len, encrypted_bin);

    unsigned char plain[bin_len];   // Decryption output cannot be longer than input data.
    int decrypted_len = 0;
    if (encrypt_or_decrypt(key.data(), iv.data(), ProcessingMode::DECRYPT_IGNORE_ERRORS, encrypted_bin,
                           bin_len, plain, &decrypted_len))
    {
        if (decrypted_len > 0)
        {
            // Success, password was encrypted using 2.5. Decrypted data should be text.
            auto output_data = reinterpret_cast<const char*>(plain);
            rval.assign(output_data, decrypted_len);
        }
        else
        {
            // Failure, password was likely encrypted in 2.4. Try to decrypt using 2.4 code.
            AES_KEY aeskey;
            AES_set_decrypt_key(key.data(), 8 * key.size(), &aeskey);
            auto iv_copy = iv;
            memset(plain, '\0', bin_len);
            AES_cbc_encrypt(encrypted_bin, plain, bin_len, &aeskey, iv_copy.data(), AES_DECRYPT);
            rval = reinterpret_cast<const char*>(plain);
        }
    }
    return rval;
}

string decrypt_password(const ByteVec& key, const std::string& input)
{
    int total_hex_len = input.length();
    string rval;

    // Extract IV.
    auto ptr = input.data();
    int iv_bin_len = secrets_ivlen();
    int iv_hex_len = 2 * iv_bin_len;
    uint8_t iv_bin[iv_bin_len];
    if (total_hex_len >= iv_hex_len)
    {
        hex2bin(ptr, iv_hex_len, iv_bin);

        int encrypted_hex_len = total_hex_len - iv_hex_len;
        int encrypted_bin_len = encrypted_hex_len / 2;
        unsigned char encrypted_bin[encrypted_bin_len];
        hex2bin(ptr + iv_hex_len, encrypted_hex_len, encrypted_bin);

        uint8_t decrypted[encrypted_bin_len];   // Decryption output cannot be longer than input data.
        int decrypted_len = 0;
        if (encrypt_or_decrypt(key.data(), iv_bin, ProcessingMode::DECRYPT, encrypted_bin, encrypted_bin_len,
                               decrypted, &decrypted_len))
        {
            // Decrypted data should be text.
            auto output_data = reinterpret_cast<const char*>(decrypted);
            rval.assign(output_data, decrypted_len);
        }
    }

    return rval;
}

/**
 * Encrypt a password that can be stored in the Columnstore configuration file.
 *
 * @param key Encryption key and init vector
 * @param input The plaintext password to encrypt.
 * @return The encrypted password, or empty on failure.
 */
string encrypt_password_old(const ByteVec& key, const ByteVec& iv, const string& input)
{
    string rval;
    // Output can be a block length longer than input.
    auto input_len = input.length();
    unsigned char encrypted_bin[input_len + AES_BLOCK_SIZE];

    // Although input is text, interpret as binary.
    auto input_data = reinterpret_cast<const uint8_t*>(input.c_str());
    int encrypted_len = 0;
    if (encrypt_or_decrypt(key.data(), iv.data(), ProcessingMode::ENCRYPT,
                           input_data, input_len, encrypted_bin, &encrypted_len))
    {
        int hex_len = 2 * encrypted_len;
        char hex_output[hex_len + 1];
        bin2hex(encrypted_bin, encrypted_len, hex_output);
        rval.assign(hex_output, hex_len);
    }
    return rval;
}

string encrypt_password(const ByteVec& key, const string& input)
{
    string rval;
    // Generate random IV.
    auto ivlen = secrets_ivlen();
    unsigned char iv_bin[ivlen];
    if (RAND_bytes(iv_bin, ivlen) != 1)
    {
        printf("OpenSSL RAND_bytes() failed. %s.\n", ERR_error_string(ERR_get_error(), nullptr));
        return rval;
    }

    // Output can be a block length longer than input.
    auto input_len = input.length();
    unsigned char encrypted_bin[input_len + EVP_CIPHER_block_size(secrets_cipher())];

    // Although input is text, interpret as binary.
    auto input_data = reinterpret_cast<const uint8_t*>(input.c_str());
    int encrypted_len = 0;
    if (encrypt_or_decrypt(key.data(), iv_bin, ProcessingMode::ENCRYPT,
                           input_data, input_len, encrypted_bin, &encrypted_len))
    {
        // Form one string with IV in front.
        int iv_hex_len = 2 * ivlen;
        int encrypted_hex_len = 2 * encrypted_len;
        int total_hex_len = iv_hex_len + encrypted_hex_len;
        char hex_output[total_hex_len + 1];
        bin2hex(iv_bin, ivlen, hex_output);
        bin2hex(encrypted_bin, encrypted_len, hex_output + iv_hex_len);
        rval.assign(hex_output, total_hex_len);
    }
    return rval;
}

bool load_encryption_keys()
{
    if(this_unit.key.empty() || this_unit.iv.empty())
    {
        string path(MCSDATADIR);
        path.append("/").append(SECRETS_FILENAME);
        auto ret = secrets_readkeys(path);
        if (ret.ok)
        {
            if (!ret.key.empty())
            {
                CSPasswdLogging::get()->log(LOG_INFO,"Using encrypted passwords. Encryption key read from '%s'.", path.c_str());
                this_unit.key = move(ret.key);
                this_unit.iv = move(ret.iv);
            }
            else
            {
                CSPasswdLogging::get()->log(LOG_INFO,"Password encryption key file '%s' not found, using configured passwords as "
                           "plaintext.", path.c_str());
            }
            return ret.ok;
        }
    }
    return true;
}

/**
 * Write encryption key to JSON-file. Also sets file permissions and owner.
 *
 * @param key Encryption key in binary form
 * @param filepath The full path to the file to write to
 * @param owner The final owner of the file. Changing the owner does not always succeed.
 * @return True on total success. Even if false is returned, the file may have been written.
 */
bool secrets_write_keys(const ByteVec& key, const string& filepath, const string& owner)
{
    auto keylen = key.size();
    char key_hex[2 * keylen + 1];
    bin2hex(key.data(), keylen, key_hex);

    boost::property_tree::ptree jsontree;
    jsontree.put(field_desc,desc);
    jsontree.put(field_version,columnstore_version.c_str());
    jsontree.put(field_cipher,CIPHER_NAME);
    jsontree.put(field_key,(const char*)key_hex);

    auto filepathc = filepath.c_str();
    bool write_ok = false;
    errno = 0;
    try
    {
        write_json(filepathc, jsontree);
    }
    catch(boost::property_tree::json_parser::json_parser_error &je)
    {
        std::cout << "Write to secrets file: " << je.filename() << " on line: " << je.line() << std::endl;
        std::cout << je.message() << std::endl;
    }
    catch(...)
    {
        printf("Write to secrets file '%s' failed. Error %d, %s.\n",
               filepathc, errno, strerror(errno));
    }
    write_ok = true;

    jsontree.clear();
    bool rval = false;
    if (write_ok)
    {
        // Change file permissions to prevent modifications.
        errno = 0;
        if (chmod(filepathc, S_IRUSR) == 0)
        {
            printf("Permissions of '%s' set to owner:read.\n", filepathc);
            auto ownerz = owner.c_str();
            auto userinfo = getpwnam(ownerz);
            if (userinfo)
            {
                if (chown(filepathc, userinfo->pw_uid, userinfo->pw_gid) == 0)
                {
                    printf("Ownership of '%s' given to %s.\n", filepathc, ownerz);
                    rval = true;
                }
                else
                {
                    printf("Failed to give '%s' ownership of '%s': %d, %s.\n",
                           ownerz, filepathc, errno, strerror(errno));
                }
            }
            else
            {
                printf("Could not find user '%s' when attempting to change ownership of '%s': %d, %s.\n",
                       ownerz, filepathc, errno, strerror(errno));
            }
        }
        else
        {
            printf("Failed to change the permissions of the secrets file '%s'. Error %d, %s.\n",
                   filepathc, errno, strerror(errno));
        }
    }
    return rval;
}
