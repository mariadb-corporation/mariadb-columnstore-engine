/* Copyright (C) 2014 InfiniDB, Inc.

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

/*******************************************************************************
* $Id: we_define.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
*
*******************************************************************************/
/** @file" **/

#include <string>

#include "we_define.h"

#include "idberrorinfo.h"
#include "errorids.h"
#include "brmtypes.h"
#include "we_brm.h"

/** Namespace WriteEngine **/
namespace WriteEngine
{

WErrorCodes::WErrorCodes() : fErrorCodes()
{
    fErrorCodes[ERR_UNKNOWN] = " a Generic (unknown) error";
    fErrorCodes[ERR_INVALID_PARAM] = " due to Invalid parameters";
    fErrorCodes[ERR_STRUCT_EMPTY] = " because the Structure is empty";
    fErrorCodes[ERR_VALUE_OUTOFRANGE] = " because a Value is out of range";
    fErrorCodes[ERR_PARSING] = " a Value is out of range";
    fErrorCodes[ERR_NO_MEM] = " out of memory";
    fErrorCodes[ERR_DML_LOG_NAME] = " construct DML log filename failed";
    fErrorCodes[ERR_OPEN_DML_LOG] = " open DML log file failed";
    fErrorCodes[ERR_HDFS_BACKUP] = " DML backup error in HDFS";

    // File level error
    fErrorCodes[ERR_FILE_CREATE] = " The column file could not be created; it may already exist or be inaccessible.";
    fErrorCodes[ERR_FILE_OPEN] = " opening a column file. The file was not found or was inaccessible.";
    fErrorCodes[ERR_FILE_DELETE] = " it can not delete the file, because it does not exist or was inaccessible. ";
    fErrorCodes[ERR_FILE_EXIST] =  " The File already exists. ";
    fErrorCodes[ERR_FILE_NOT_EXIST] = " The File does not exist. " ;
    fErrorCodes[ERR_FILE_NULL] =  " The FILE pointer is null." ;
    fErrorCodes[ERR_FILE_WRITE] = " Error writing to a database file. ";
    fErrorCodes[ERR_FILE_READ] = " Error reading from a database file. ";
    fErrorCodes[ERR_FILE_SEEK] = " Error in positioning file handle. ";
    fErrorCodes[ERR_FILE_READ_IMPORT] = " Error reading import source file. ";
    fErrorCodes[ERR_DIR_CREATE] = " Error in creating a directory. ";
    fErrorCodes[ERR_FILE_NEW_EXTENT_FBO] = " New extent FBO too high for current file. ";
    fErrorCodes[ERR_FILE_FBO_NEG] = " Specified file FBO is negative. ";
    fErrorCodes[ERR_FILE_TRUNCATE] = " Error truncating db file. ";
    fErrorCodes[ERR_FILE_DISK_SPACE] = "Not able to add extent; adding extent "
                                       "would exceed max file system disk usage. ";
    fErrorCodes[ERR_FILE_STAT] = " Error getting stats on db file. ";
    fErrorCodes[ERR_VB_FILE_NOT_EXIST] = " Version buffer file  does not exists.";
    fErrorCodes[ERR_FILE_FLUSH] = " Error flushing db file. ";
    fErrorCodes[ERR_FILE_GLOBBING] = " Error globbing a file name. ";

    // XML level error
    fErrorCodes[ERR_XML_FILE] = " An xml file error, usually because the file does not exist";
    fErrorCodes[ERR_XML_ROOT_ELEM] =  " An xml file Root element error ";
    fErrorCodes[ERR_XML_EMPTY] =  " An Empty XML file ";
    fErrorCodes[ERR_XML_PARSE] = " An XML Parsing error";

    // table lock level error
    fErrorCodes[ERR_TBLLOCK_LOCK_NOT_FOUND] = "Table is not locked.";
    fErrorCodes[ERR_TBLLOCK_GET_LOCK]      = "Error getting table lock.";
    fErrorCodes[ERR_TBLLOCK_GET_LOCK_LOCKED] = "Table locked by another user.";
    fErrorCodes[ERR_TBLLOCK_RELEASE_LOCK]  = "Error releasing table lock.";
    fErrorCodes[ERR_TBLLOCK_CHANGE_STATE]  = "Error changing table lock state.";
    fErrorCodes[ERR_TBLLOCK_GET_INFO]      = "Error getting table lock info.";
    fErrorCodes[ERR_TBLLOCK_LOCKID_CONFLICT] = "Table LockID for different table than expected.";

    // DDL/DML Interface level error
    fErrorCodes[ERR_STRUCT_VALUE_NOT_MATCH] = " the number of structs does not match with the number of value sets";
    fErrorCodes[ERR_ROWID_VALUE_NOT_MATCH] = " the number of rowids does not match with the number of values";
    fErrorCodes[ERR_TBL_SYSCAT_ERROR]      = "Error occured when querying systemcatalog.";

    // index error
    fErrorCodes[ERR_IDX_TREE_MOVE_ENTRY] = " an error in moving part of an index tree to a new subblock";
    fErrorCodes[ERR_IDX_TREE_INVALID_TYPE] = " an Invalid index tree entry type";
    fErrorCodes[ERR_IDX_TREE_BITTEST_VAL] = " a Wrong bit test value in the index tree entry";
    fErrorCodes[ERR_IDX_TREE_INVALID_LEVEL] = " an Invalid testbit index tree level";
    fErrorCodes[ERR_IDX_TREE_INVALID_GRP] = "an Invalid index tree group type ";
    fErrorCodes[ERR_IDX_TREE_LISTPTR_CHANGE] = " an index tree List pointer change";
    //index list error
    fErrorCodes[ERR_IDX_LIST_INVALID_ADDHDR] = " a Create indexlist header error";
    fErrorCodes[ERR_IDX_LIST_INVALID_UPDATE] = " an pdate Index List error ";
    fErrorCodes[ERR_IDX_LIST_INVALID_DELETE] = " a Delete rowid in indexlist err";
    fErrorCodes[ERR_IDX_LIST_INVALID_KEY] =  " an Invalid index listbppseeder.cpp Key passed";
    fErrorCodes[ERR_IDX_LIST_GET_RID_ARRARY] = " an index list RID array";
    fErrorCodes[ERR_IDX_LIST_WRONG_KEY ] = " a not matched Key passed to an index list";
    fErrorCodes[ERR_IDX_LIST_HDR_EMPTY] = " an empty index list header";
    fErrorCodes[ERR_IDX_LIST_GET_SEGMT] = " in an index list Get Segment";
    fErrorCodes[ERR_IDX_LIST_WRONG_LBID_WRITE] = " an index list incorrect LBID write";
    fErrorCodes[ERR_IDX_LIST_UPDATE_SUB] = " in an index list update sub";
    fErrorCodes[ERR_IDX_LIST_UPDATE_NARRAY] = " in an index list update narray";
    fErrorCodes[ERR_IDX_LIST_LAST_FBO_NEG] = " the last index list FBO neg";
    fErrorCodes[ERR_IDX_LIST_INIT_NEW_BLKS] = " in an index list initialize new blocks";
    fErrorCodes[ERR_IDX_LIST_INIT_LINK_BLKS] = " in an index list initialize link blocks";
    fErrorCodes[ERR_IDX_LIST_UPDATE_COUNT] = " in an index list update count";
    fErrorCodes[ERR_IDX_LIST_SET_NEXT_LBID] = " in an index list set next LBID";
    fErrorCodes[ERR_IDX_LIST_INVALID_LBID] = "an index list invalid LBID";
    fErrorCodes[ERR_IDX_LIST_INVALID_BLK_READ] = " in an index list invalid LBID read";
    fErrorCodes[ERR_IDX_LIST_UPDATE_HDR_COUNT] = " in an index list update header count";
    fErrorCodes[ERR_IDX_LIST_WRONG_BLK] = " an index list wrong block";
    fErrorCodes[ERR_IDX_LIST_WRONG_TYPE] = " an index list wrong type";
    fErrorCodes[ERR_IDX_LIST_GET_COUNT] = " in an index list get count";
    fErrorCodes[ERR_IDX_LIST_GET_NEXT] = " in an index list get next";
    fErrorCodes[ERR_IDX_LIST_GET_PARENT] = " in an index list get parent";
    fErrorCodes[ERR_IDX_LIST_GET_SUB_BLK] = " in an index list get sub block";
    fErrorCodes[ERR_IDX_LIST_INVALID_UP_HDR] = " an invalid Update Index List header ";
    fErrorCodes[ERR_IDX_LIST_INVALID_ADD_LIST] = " an invalid add Index List";
    fErrorCodes[ERR_IDX_LIST_INVALID_UP] = " an invalid Update Index List";

    //freemgr error
    fErrorCodes[ERR_FM_ASSIGN_ERR] = " in an assignment";
    fErrorCodes[ERR_FM_RELEASE_ERR] = " in a release";
    fErrorCodes[ERR_FM_BAD_FBO] = " an invalid File Block Offset";
    fErrorCodes[ERR_FM_BAD_TYPE] = "an invalid type that must be pointer or list";
    fErrorCodes[ERR_FM_NO_SPACE] = " that No blocks are available";
    fErrorCodes[ERR_FM_EXTEND] = " while extending a file";

    // Dictionary error
    fErrorCodes[ERR_DICT_NO_SPACE_INSERT] = " no space for a dictionary insert";
    fErrorCodes[ERR_DICT_SIZE_GT_2G] = " the dictionary size was > 2GB";
    fErrorCodes[ERR_DICT_NO_OP_DELETE] = " in the dictionary no op delete";
    fErrorCodes[ERR_DICT_NO_OFFSET_DELETE] = " a dictionary bad Delete offset";
    fErrorCodes[ERR_DICT_INVALID_HDR] = " a dictionary bad Delete Hdr";
    fErrorCodes[ERR_DICT_ZERO_LEN] = " a dictionary zero len";
    fErrorCodes[ERR_DICT_TOKEN_NOT_FOUND] = " a dictionary token not found";
    fErrorCodes[ERR_DICT_FILE_NOT_FOUND] = " a dictionary file not found";
    fErrorCodes[ERR_DICT_BAD_TOKEN_LBID] = " a dictionary token lbid is bad";
    fErrorCodes[ERR_DICT_BAD_TOKEN_OP] = " a dictionary token op is bad";

    // Bulk error
    fErrorCodes[ERR_BULK_MAX_ERR_NUM] = " the Maximum number of error rows reached";
    fErrorCodes[ERR_BULK_DATA_COL_NUM] =  " the total number of data column not match with column definitions";
    fErrorCodes[ERR_BULK_SEND_MSG_ERR] = " in a bulk load send msg";
    fErrorCodes[ERR_BULK_MISSING_EXTENT_ENTRY] = " missing Extent Entry when trying to save LBID info for CP";
    fErrorCodes[ERR_BULK_MISSING_EXTENT_ROW] = " missing Extent Row when trying to save LBID info for CP";
    fErrorCodes[ERR_BULK_ROW_FILL_BUFFER] = " Single row fills read buffer; try larger read buffer.";
    fErrorCodes[ERR_BULK_DBROOT_CHANGE] = " Local PM DBRoot settings changed during bulk load.";
    fErrorCodes[ERR_BULK_ROLLBACK_MISS_ROOT] = " Mode3 automatic rollback not performed. DBRoot missing.";
    fErrorCodes[ERR_BULK_ROLLBACK_SEG_LIST] = " Error building segment file list in a directory.";
    fErrorCodes[ERR_BULK_BINARY_PARTIAL_REC] = " Binary import did not end on fixed length record boundary.";
    fErrorCodes[ERR_BULK_BINARY_IGNORE_FLD] = " <IgnoreField> tag not supported for binary imports.";

    // BRM error
    fErrorCodes[ERR_BRM_LOOKUP_LBID] = " a BRM Lookup LBID error.";
    fErrorCodes[ERR_BRM_LOOKUP_FBO] = " a BRM Lookup FBO error.";
    fErrorCodes[ERR_BRM_ALLOC_EXTEND] = " a BRM Allocate extent error.";
    fErrorCodes[ERR_BRM_COMMIT] = " a BRM Commit error.";
    fErrorCodes[ERR_BRM_ROLLBACK] = " a BRM Rollback error.";
    fErrorCodes[ERR_BRM_GET_UNCOMM_LBID] = " a BRM get uncommitted lbid list error.";
    fErrorCodes[ERR_BRM_DEL_OID] = " a BRM Delete oid error.";
    fErrorCodes[ERR_BRM_BEGIN_COPY] = " a BRM Begin copy error.";
    fErrorCodes[ERR_BRM_END_COPY] = " a BRM End copy error.";
    fErrorCodes[ERR_BRM_GET_HWM] = " a BRM get hwm error.";
    fErrorCodes[ERR_BRM_SET_HWM] = " a BRM Set hwm error.";
    fErrorCodes[ERR_BRM_WR_VB_ENTRY] = " a BRM VB entry error.";
    fErrorCodes[ERR_BRM_VB_COPY_READ] = " a BRM VB copy read error.";
    fErrorCodes[ERR_BRM_VB_COPY_SEEK_DB] = " a BRM VB copy seek error against DB file.";
    fErrorCodes[ERR_BRM_VB_COPY_SEEK_VB] = " a BRM VB copy seek error against VB file.";
    fErrorCodes[ERR_BRM_VB_COPY_WRITE] = " a BRM VB copy write.";
    fErrorCodes[ERR_BRM_DEAD_LOCK] = " a BRM DEAD lock error.";
    fErrorCodes[ERR_BRM_MARK_INVALID] = " a BRM Mark extent invalid error from casual paritioning.";
    fErrorCodes[ERR_BRM_SAVE_STATE] = " a BRM Save state error.";
    fErrorCodes[ERR_BRM_GET_START_EXTENT] = " a BRM get start Extent error.";
    fErrorCodes[ERR_BRM_VB_OVERFLOW] = "BRM block version buffer overflow error.";
    fErrorCodes[ERR_BRM_READ_ONLY]   = "BRM is in read-only state.";
    fErrorCodes[ERR_BRM_GET_READ_WRITE] = "BRM error getting read-write state.";
    fErrorCodes[ERR_BRM_BULK_RB_COLUMN] = "BRM error performing bulk rollback of column extents.";
    fErrorCodes[ERR_BRM_BULK_RB_DCTNRY] = "BRM error performing bulk rollback of dictionary store extents.";
    fErrorCodes[ERR_BRM_DELETE_EXTENT_COLUMN] = "BRM error deleting column extents.";
    fErrorCodes[ERR_BRM_DELETE_EXTENT_DCTNRY] = "BRM error deleting dictionary extents.";
    fErrorCodes[ERR_BRM_TAKE_SNAPSHOT] = "BRM error requesting snapshot of BRM state.";
    fErrorCodes[ERR_BRM_LOOKUP_START_LBID] = "BRM start LBID lookup error.";
    fErrorCodes[ERR_BRM_BULK_UPDATE] = "BRM error executing bulk update of HWM and CP.";
    fErrorCodes[ERR_BRM_GET_EXT_STATE] = "BRM error getting segment file extent state.";
    fErrorCodes[ERR_EXTENTMAP_LOOKUP] = " a extent map Lookup error.";
    fErrorCodes[ERR_BRM_LOOKUP_VERSION] = " a vssLookup version info error.";
    fErrorCodes[ERR_BRM_LOOKUP_LBID_RANGES] = " BRM error getting LBID ranges.";
    fErrorCodes[ERR_BRM_HWMS_NOT_EQUAL] = " HWMs for same width columns not equal. ";
    fErrorCodes[ERR_BRM_HWMS_OUT_OF_SYNC] = " HWMs for different width columns not in sync. ";
    fErrorCodes[ERR_BRM_DBROOT_HWMS] = " BRM error getting HWMs for DBRoots. ";
    fErrorCodes[ERR_BRM_NETWORK] = " Network error in DBRM call. ";
    fErrorCodes[ERR_BRM_READONLY] = " DBRM is read only. ";
    fErrorCodes[ERR_INVALID_VBOID] = " The VB oid is invalid ";
    fErrorCodes[ERR_BRM_SET_EXTENTS_CP] = " BRM error setting extents min/max ";
    fErrorCodes[ERR_BRM_SHUTDOWN] = " The system is being shutdown ";
    fErrorCodes[ERR_BRM_GET_SHUTDOWN] = " BRM error get the system shutdown flag ";
    fErrorCodes[ERR_BRM_SUSPEND] = " The system is in write suspended mode";
    fErrorCodes[ERR_BRM_GET_SUSPEND] = " BRM error get the system suspend flag ";
    fErrorCodes[ERR_BRM_BAD_STRIPE_CNT] = " Incorrect number of column extents allocated in stripe";

    // DM error
    fErrorCodes[ERR_DM_CONVERT_OID] = " a DM Conversion error";

    // Cache error
    fErrorCodes[ERR_CACHE_KEY_EXIST ] = " a Cache key exists";
    fErrorCodes[ERR_CACHE_KEY_NOT_EXIST] = " a Cache key does not exist";
    fErrorCodes[ERR_NULL_BLOCK] = " a Block is NULL";
    fErrorCodes[ERR_FREE_LIST_EMPTY] = " a Free list is empty";

    // Compression error
    fErrorCodes[ERR_COMP_COMPRESS] = " Error in compressing data. ";
    fErrorCodes[ERR_COMP_UNCOMPRESS] = " Error in uncompressing data. ";
    fErrorCodes[ERR_COMP_PARSE_HDRS] = " Error parsing compression headers. ";
    fErrorCodes[ERR_COMP_VERIFY_HDRS] = " Error verifying compression headers. ";
    fErrorCodes[ERR_COMP_PAD_DATA] = " Error in padding compressed data. ";
    fErrorCodes[ERR_COMP_READ_BLOCK] = " Error in reading a data block. ";
    fErrorCodes[ERR_COMP_SAVE_BLOCK] = " Error in saving a data block. ";
    fErrorCodes[ERR_COMP_WRONG_PTR] = " Invalid pointer in compression headers. ";
    fErrorCodes[ERR_COMP_FILE_NOT_FOUND] = " Error searching for a compressed file. ";
    fErrorCodes[ERR_COMP_CHUNK_NOT_FOUND] = " Error searching for a compressed chunk. ";
    fErrorCodes[ERR_COMP_UNAVAIL_TYPE] = " Unavailable compressino type. ";
    fErrorCodes[ERR_COMP_REMOVE_FILE] = " Failed to remove a file. ";
    fErrorCodes[ERR_COMP_RENAME_FILE] = " Failed to rename a file. ";
    fErrorCodes[ERR_COMP_OPEN_FILE] = " Failed to open a compressed data file. ";
    fErrorCodes[ERR_COMP_SET_OFFSET] = " Failed to set offset in a compressed data file. ";
    fErrorCodes[ERR_COMP_READ_FILE] = " Failed to read from a compressed data file. ";
    fErrorCodes[ERR_COMP_WRITE_FILE] = " Failed to write to a compresssed data file. ";
    fErrorCodes[ERR_COMP_CLOSE_FILE] = " Failed to close a compressed data file. ";
    fErrorCodes[ERR_COMP_TRUNCATE_ZERO] = " Attempting to truncate compressed file to 0 bytes. ";

    // Auto-increment error
    fErrorCodes[ERR_AUTOINC_GEN_EXCEED_MAX] = " Generated auto-increment value "
            "exceeds maximum value for the column type.";
    fErrorCodes[ERR_AUTOINC_USER_OUT_OF_RANGE] = " User specified auto-"
            "increment value is out of range for the column type.";
    fErrorCodes[ERR_AUTOINC_TABLE_NAME] = " Invalid schema/tablename for auto increment. ";
    fErrorCodes[ERR_AUTOINC_INIT1] = " Unable to initialize auto-increment value. ";
    fErrorCodes[ERR_AUTOINC_INIT2] = " Unable to initialize auto-increment value. Unknown exception. ";
    fErrorCodes[ERR_AUTOINC_RID] = " Failed to get row information from calpontsystemcatalog.";
    fErrorCodes[ERR_AUTOINC_START_SEQ] = " Unable to setup AI sequence in BRM.";
    fErrorCodes[ERR_AUTOINC_GET_RANGE] = " Unable to reserve AI range from BRM.";
    fErrorCodes[ERR_AUTOINC_GET_LOCK]  = " Unable to lock AI column in BRM.";
    fErrorCodes[ERR_AUTOINC_REL_LOCK]  = " Unable to release AI column in BRM.";
    fErrorCodes[ERR_AUTOINC_UPDATE] = " Unable to update nextValue in system catalog.";

    // Block cache flush error
    fErrorCodes[ERR_BLKCACHE_FLUSH_LIST] = " Failed to flush list of blocks from PrimProc cache. ";

    // Backup bulk meta data file error
    fErrorCodes[ERR_METADATABKUP_FILE_RENAME]         = " Unable to rename temporary bulk meta data file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_PARSE_HDRS]     = " Error parsing compression headers in bulk backup file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_VERIFY_HDRS]    = " Error verifying compression headers in bulk backup file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_CHUNK_NOT_FOUND] = " Error searching for compressed chunk in db file being backed up. ";
    fErrorCodes[ERR_METADATABKUP_COMP_OPEN_BULK_BKUP] = " Error opening compressed chunk in bulk backup file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_WRITE_BULK_BKUP] = " Error writing compressed chunk to bulk backup file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_READ_BULK_BKUP] = " Error reading compressed chunk from bulk backup file. ";
    fErrorCodes[ERR_METADATABKUP_COMP_RENAME]         = " Unable to rename compressed chunk bulk backup file. ";
}

std::string WErrorCodes::errorString(int code)
{
    // Look for error message overrides from system-wide error messages
    switch (code)
    {
        case ERR_FILE_DISK_SPACE:
        {
            logging::Message::Args args;
            std::string msgArg; // empty str arg; no extra info in this context
            args.add( msgArg );
            return logging::IDBErrorInfo::instance()->errorMsg(
                       logging::ERR_EXTENT_DISK_SPACE, args);
            break;
        }
    }

    int brmRc = BRMWrapper::getBrmRc();

    if (brmRc == BRM::ERR_OK)
        return (fErrorCodes[code]);

    std::string errMsg( fErrorCodes[code] );
    std::string brmMsg;
    errMsg += " [BRM error status: ";
    BRM::errString(brmRc, brmMsg);
    errMsg += brmMsg;
    errMsg += "]";

    return errMsg;
}

} //end of namespace
