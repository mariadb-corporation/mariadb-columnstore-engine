#ifndef COMPAT_BOOST_FILESYSTEM_HPP
#define COMPAT_BOOST_FILESYSTEM_HPP

#include <boost/version.hpp>

// copy_option was replaced with copy_options in version 1.74.0 released 2020-08-14
// copy_option was removed in version 1.85.0 released 2024-04-15
#if BOOST_VERSION < 107400

#include <boost/filesystem/operations.hpp>

namespace boost
{
namespace filesystem
{
namespace copy_options
{

constexpr copy_option::enum_type overwrite_existing = copy_option::overwrite_if_exists;
constexpr copy_option::enum_type none = copy_option::fail_if_exists;

}
}
}

#endif

#endif
