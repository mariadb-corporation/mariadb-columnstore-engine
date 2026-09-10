# Third-party source code vendored (copied) into the ColumnStore tree.
#
# The server's SBOM generator (cmake/generate_sbom.cmake in the server repo)
# only knows git submodules and its own hand-written table, so vendored code
# is invisible to it unless declared here. The generator accepts extra
# components through EXTRA_SBOM_DEPENDENCIES plus per-component variables
# (<name>_URL/_TAG/_REVISION/_VERSION/_DESCRIPTION, <repo>.license,
# <repo>.copyright, <repo>_PURL). ColumnStore is configured before
# GENERATE_SBOM() runs, so exporting them as CACHE INTERNAL variables is
# enough - no server-side change needed.
#
# Versions are parsed from the vendored headers where a version macro exists,
# so a header bump can never ship a stale SBOM version. Keep this list and
# THIRD-PARTY-NOTICES in sync: every entry here has a section there.

# Read one "#define <macro> <number>" from a header, fail loudly otherwise.
function(columnstore_header_define header macro out_var)
    file(STRINGS "${header}" line REGEX "^#define[ \t]+${macro}[ \t]+[0-9]+")
    if(NOT line)
        message(FATAL_ERROR "SBOM: '${macro}' not found in ${header}")
    endif()
    string(REGEX REPLACE "^#define[ \t]+${macro}[ \t]+([0-9]+).*$" "\\1" value "${line}")
    if(NOT value MATCHES "^[0-9]+$")
        message(FATAL_ERROR "SBOM: cannot parse '${macro}' from '${line}' in ${header}")
    endif()
    set(${out_var} "${value}" PARENT_SCOPE)
endfunction()

# Register one vendored component for the server SBOM generator.
# <name> must be the upstream repository name (the generator derives the
# license/copyright variable prefix from the last URL component).
function(columnstore_sbom_vendored name url tag version license copyright description)
    set(${name}_URL "${url}" CACHE INTERNAL "SBOM: upstream URL of vendored ${name}")
    set(${name}_TAG "${tag}" CACHE INTERNAL "SBOM: upstream tag of vendored ${name}")
    set(${name}_REVISION "${tag}" CACHE INTERNAL "SBOM: upstream revision of vendored ${name}")
    set(${name}_VERSION "${version}" CACHE INTERNAL "SBOM: version of vendored ${name}")
    set(${name}_DESCRIPTION "${description}" CACHE INTERNAL "SBOM: description of vendored ${name}")
    set("${name}.license" "${license}" CACHE INTERNAL "SBOM: SPDX license id of vendored ${name}")
    set("${name}.copyright" "${copyright}" CACHE INTERNAL "SBOM: copyright notice of vendored ${name}")
    # Rebuild the exported list on every configure: drop our own name first so
    # re-configuring the same build dir never duplicates an entry, but keep
    # anything the user passed with -DEXTRA_SBOM_DEPENDENCIES=...
    set(deps ${EXTRA_SBOM_DEPENDENCIES})
    list(REMOVE_ITEM deps "${name}")
    list(APPEND deps "${name}")
    set(EXTRA_SBOM_DEPENDENCIES "${deps}" CACHE INTERNAL "Extra components for SBOM generation" FORCE)
endfunction()

set(_vendored_root "${CMAKE_CURRENT_LIST_DIR}/..")

# nlohmann/json - utils/json/json.hpp (single header)
columnstore_header_define("${_vendored_root}/utils/json/json.hpp" NLOHMANN_JSON_VERSION_MAJOR _json_major)
columnstore_header_define("${_vendored_root}/utils/json/json.hpp" NLOHMANN_JSON_VERSION_MINOR _json_minor)
columnstore_header_define("${_vendored_root}/utils/json/json.hpp" NLOHMANN_JSON_VERSION_PATCH _json_patch)
set(_json_version "${_json_major}.${_json_minor}.${_json_patch}")
columnstore_sbom_vendored(
    json "https://github.com/nlohmann/json" "v${_json_version}" "${_json_version}" "MIT"
    "Copyright (c) 2013-2023 Niels Lohmann" "JSON for Modern C++, vendored into ColumnStore as utils/json/json.hpp"
)

# robin-hood-hashing - utils/common/robin_hood.h (single header)
columnstore_header_define("${_vendored_root}/utils/common/robin_hood.h" ROBIN_HOOD_VERSION_MAJOR _rh_major)
columnstore_header_define("${_vendored_root}/utils/common/robin_hood.h" ROBIN_HOOD_VERSION_MINOR _rh_minor)
columnstore_header_define("${_vendored_root}/utils/common/robin_hood.h" ROBIN_HOOD_VERSION_PATCH _rh_patch)
set(_rh_version "${_rh_major}.${_rh_minor}.${_rh_patch}")
columnstore_sbom_vendored(
    robin-hood-hashing "https://github.com/martinus/robin-hood-hashing" "${_rh_version}" "${_rh_version}" "MIT"
    "Copyright (c) 2018-2020 Martin Ankerl"
    "robin_hood unordered map and set, vendored into ColumnStore as utils/common/robin_hood.h"
)

# JPCRE2 - utils/pcre2/jpcre2.hpp (single header). JPCRE2_VERSION is one
# number MMmmpp (103201 = 10.32.01); upstream tags use the dotted form.
columnstore_header_define("${_vendored_root}/utils/pcre2/jpcre2.hpp" JPCRE2_VERSION _jpcre2_num)
math(EXPR _jpcre2_major "${_jpcre2_num} / 10000")
math(EXPR _jpcre2_minor "(${_jpcre2_num} / 100) % 100")
math(EXPR _jpcre2_patch "${_jpcre2_num} % 100")
foreach(part _jpcre2_minor _jpcre2_patch)
    if(${part} LESS 10)
        set(${part} "0${${part}}")
    endif()
endforeach()
set(_jpcre2_version "${_jpcre2_major}.${_jpcre2_minor}.${_jpcre2_patch}")
columnstore_sbom_vendored(
    jpcre2 "https://github.com/jpcre2/jpcre2" "${_jpcre2_version}" "${_jpcre2_version}" "BSD-3-Clause"
    "Copyright (c) Md. Jahidul Hamid" "C++ wrapper for PCRE2, vendored into ColumnStore as utils/pcre2/jpcre2.hpp"
)

# utfcpp - utils/funcexp/utf8.h + utils/funcexp/utf8/{core,checked,unchecked}.h.
# No version macro. Pinned to 2.3.3 by content comparison against upstream
# 2.3.2 and 2.3.4 (MCOL-6567); 2.3.3 predates upstream's GitHub history, so
# there is no GitHub tag to point the purl at.
set(_utfcpp_version "2.3.3")
set(utfcpp_PURL "pkg:generic/utfcpp@${_utfcpp_version}" CACHE INTERNAL "SBOM: purl of vendored utfcpp")
columnstore_sbom_vendored(
    utfcpp "https://github.com/nemtrif/utfcpp" "${_utfcpp_version}" "${_utfcpp_version}" "BSL-1.0"
    "Copyright 2006 Nemanja Trifunovic"
    "UTF-8 with C++ in a Portable Way, vendored into ColumnStore as utils/funcexp/utf8*, with local edits in core.h"
)

# PicoSAT - tools/rebuildEM/picosat.c + picosat.h, compiled into mcsRebuildEM.
# Release 965 with local edits. No version macro in the source, so the version
# is fixed here. Upstream is https://fmv.jku.at/picosat/ (not GitHub), hence
# the generic purl. The URL below is only parsed by the generator for the
# component name (last segment) and supplier (first segment).
set(_picosat_version "965")
set(picosat_PURL "pkg:generic/picosat@${_picosat_version}" CACHE INTERNAL "SBOM: purl of vendored picosat")
columnstore_sbom_vendored(
    picosat "https://fmv.jku.at/picosat/picosat" "${_picosat_version}" "${_picosat_version}" "MIT"
    "Copyright (c) 2006 - 2015, Armin Biere, Johannes Kepler University"
    "PicoSAT SAT solver, vendored into ColumnStore as tools/rebuildEM/picosat.c and linked into mcsRebuildEM"
)

unset(_vendored_root)
