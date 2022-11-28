/*****************************************************************************

Copyright (c) 1997, 2016, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2016, 2022, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file ibuf/ibuf0ibuf.cc
Insert buffer

Created 7/19/1997 Heikki Tuuri
*******************************************************/

#include "ibuf0ibuf.h"
#include "btr0sea.h"

/** Number of bits describing a single page */
#define IBUF_BITS_PER_PAGE	4
/** The start address for an insert buffer bitmap page bitmap */
#define IBUF_BITMAP		PAGE_DATA

#include "buf0buf.h"
#include "buf0rea.h"
#include "fsp0fsp.h"
#include "trx0sys.h"
#include "fil0fil.h"
#include "rem0rec.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "btr0btr.h"
#include "row0upd.h"
#include "fut0lst.h"
#include "lock0lock.h"
#include "log0recv.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "log.h"

/* Possible operations buffered in the insert/whatever buffer.
DO NOT CHANGE THE VALUES OF THESE, THEY ARE STORED ON DISK. */
enum ibuf_op_t {
	IBUF_OP_INSERT = 0,
	IBUF_OP_DELETE_MARK = 1,
	IBUF_OP_DELETE = 2,

	/* Number of different operation types. */
	IBUF_OP_COUNT = 3
};

#if 0 // FIXME
static
#endif
/** the index of the change buffer tree */
dict_index_t *ibuf_index;

constexpr const page_id_t ibuf_root{0, FSP_IBUF_TREE_ROOT_PAGE_NO};
constexpr const page_id_t ibuf_header{0, FSP_IBUF_HEADER_PAGE_NO};
constexpr const index_id_t ibuf_index_id{0xFFFFFFFF00000000ULL};

/*	STRUCTURE OF AN INSERT BUFFER RECORD

MySQL 3.23 and 4.0:

1. The first field is the page number.
2. The second field is an array which stores type info for each subsequent
   field. We store the information which affects the ordering of records, and
   also the physical storage size of an SQL NULL value. E.g., for CHAR(10) it
   is 10 bytes.
3. Next we have the fields of the actual index record.

MySQL 4.1:

Note that contary to what we planned in the 1990's, there will only be one
insert buffer tree, and that is in the system tablespace of InnoDB.

1. The first field is the space id.
2. The second field is a one-byte marker (0) which differentiates records from
   the < 4.1.x storage format.
3. The third field is the page number.
4. The fourth field contains the type info, where we have also added 2 bytes to
   store the charset. In the compressed table format of 5.0.x we must add more
   information here so that we can build a dummy 'index' struct which 5.0.x
   can use in the binary search on the index page in the ibuf merge phase.
5. The rest of the fields contain the fields of the actual index record.

MySQL 5.0 (starting with 5.0.3) and 5.1:

The first byte of the fourth field is an additional marker (0) if the record
is in the compact format.  The presence of this marker can be detected by
looking at the length of the field modulo DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE.

The high-order bit of the character set field in the type info is the
"nullable" flag for the field.

MySQL 5.5 and MariaDB 5.5 and later:

The optional marker byte at the start of the fourth field is replaced by
mandatory 3 fields, totaling 4 bytes:

 1. 2 bytes: Counter field, used to sort records within a (space id, page
    no) in the order they were added. This is needed so that for example the
    sequence of operations "INSERT x, DEL MARK x, INSERT x" is handled
    correctly.

 2. 1 byte: Operation type (see ibuf_op_t).

 3. 1 byte: Flags. Currently only one flag exists, IBUF_REC_COMPACT.
*/

/** @name Offsets to the per-page bits in the insert buffer bitmap */
/* @{ */
#define	IBUF_BITMAP_FREE	0	/*!< Bits indicating the
					amount of free space */
#define IBUF_BITMAP_BUFFERED	2	/*!< TRUE if there are buffered
					changes for the page */
#define IBUF_BITMAP_IBUF	3	/*!< TRUE if page is a part of
					the ibuf tree, excluding the
					root page, or is in the free
					list of the ibuf */
/* @} */

#define IBUF_REC_FIELD_SPACE	0	/*!< in the pre-4.1 format,
					the page number. later, the space_id */
#define IBUF_REC_FIELD_MARKER	1	/*!< starting with 4.1, a marker
					consisting of 1 byte that is 0 */
#define IBUF_REC_FIELD_PAGE	2	/*!< starting with 4.1, the
					page number */
#define IBUF_REC_FIELD_METADATA	3	/* the metadata field */
#define IBUF_REC_FIELD_USER	4	/* first user field */

/* Various constants for checking the type of an ibuf record and extracting
data from it. For details, see the description of the record format at the
top of this file. */

/** @name Format of the IBUF_REC_FIELD_METADATA of an insert buffer record
The fourth column in the MySQL 5.5 format contains an operation
type, counter, and some flags. */
/* @{ */
#define IBUF_REC_INFO_SIZE	4	/*!< Combined size of info fields at
					the beginning of the fourth field */

/* Offsets for the fields at the beginning of the fourth field */
#define IBUF_REC_OFFSET_COUNTER	0	/*!< Operation counter */
#define IBUF_REC_OFFSET_TYPE	2	/*!< Type of operation */
#define IBUF_REC_OFFSET_FLAGS	3	/*!< Additional flags */

/* Record flag masks */
#define IBUF_REC_COMPACT	0x1	/*!< Set in
					IBUF_REC_OFFSET_FLAGS if the
					user index is in COMPACT
					format or later */


/** The area in pages from which contract looks for page numbers for merge */
const ulint		IBUF_MERGE_AREA = 8;

/** Inside the merge area, pages which have at most 1 per this number less
buffered entries compared to maximum volume that can buffered for a single
page are merged along with the page whose buffer became full */
const ulint		IBUF_MERGE_THRESHOLD = 4;

/** In ibuf_contract at most this number of pages is read to memory in one
batch, in order to merge the entries for them in the insert buffer */
const ulint		IBUF_MAX_N_PAGES_MERGED = IBUF_MERGE_AREA;

/* TODO: how to cope with drop table if there are records in the insert
buffer for the indexes of the table? Is there actually any problem,
because ibuf merge is done to a page when it is read in, and it is
still physically like the index page even if the index would have been
dropped! So, there seems to be no problem. */

dberr_t ibuf_cleanup()
{
  mtr_t mtr;
  mtr.start();
  mtr.x_lock_space(fil_system.sys_space);
  dberr_t err;
  buf_block_t *header_page=
    buf_page_get_gen(ibuf_header, 0, RW_X_LATCH, nullptr, BUF_GET, &mtr, &err);

  if (!header_page)
  {
  err_exit:
    sql_print_error("InnoDB: The change buffer is corrupted");
  func_exit:
    mtr.commit();
    return err;
  }

  buf_block_t *root= buf_page_get_gen(ibuf_root, 0, RW_X_LATCH, nullptr,
                                      BUF_GET, &mtr, &err);
  if (!root)
    goto err_exit;

  if (!page_has_siblings(root->page.frame) &&
      !memcmp(root->page.frame + FIL_PAGE_TYPE, field_ref_zero,
              srv_page_size - (FIL_PAGE_DATA_END + FIL_PAGE_TYPE)))
    goto func_exit; // the change buffer was removed

  if (page_is_comp(root->page.frame) ||
      btr_page_get_index_id(root->page.frame) != ibuf_index_id ||
      fil_page_get_type(root->page.frame) != FIL_PAGE_INDEX)
  {
    err= DB_CORRUPTION;
    goto err_exit;
  }

  if (srv_read_only_mode)
  {
    err= DB_READ_ONLY;
    sql_print_error("InnoDB: innodb_read_only_mode prevents an upgrade");
    goto func_exit;
  }

  sql_print_information("InnoDB: Removing the change buffer");
  abort(); // TODO: implement the upgrade
  mtr.commit();

#ifdef BTR_CUR_HASH_ADAPT
  const bool ahi = btr_search_enabled;
  if (ahi)
    btr_search_disable();
#endif

  ibuf_index=
    dict_mem_index_create(dict_table_t::create({C_STRING_WITH_LEN("ibuf")},
                                               fil_system.sys_space,
                                               1, 0, 0, 0),
                          "CLUST_IND", DICT_IBUF, 1);
  ibuf_index->id= ibuf_index_id;
  ibuf_index->n_uniq= REC_MAX_N_FIELDS;
  ibuf_index->lock.SRW_LOCK_INIT(index_tree_rw_lock_key);
#ifdef BTR_CUR_ADAPT
  ibuf_index->search_info= btr_search_info_create(ibuf_index->heap);
#endif /* BTR_CUR_ADAPT */
  ibuf_index->page = FSP_IBUF_TREE_ROOT_PAGE_NO;
  ut_d(ibuf_index->cached = TRUE);

#if defined UNIV_DEBUG
  mtr.start();
  btr_pcur_t pcur;
  if (DB_SUCCESS == pcur.open_leaf(true, ibuf_index, BTR_SEARCH_LEAF, &mtr))
  {
    while (btr_pcur_move_to_next_user_rec(&pcur, &mtr))
      rec_print_old(stderr, btr_pcur_get_rec(&pcur));
  }
  mtr.commit();
#endif

#ifdef BTR_CUR_HASH_ADAPT
  if (ahi)
    btr_search_enable();
#endif

  dict_table_t *ibuf_table= ibuf_index->table;
  ibuf_index->lock.free();
  dict_mem_index_free(ibuf_index);
  dict_mem_table_free(ibuf_table);
  ibuf_index= nullptr;
  return DB_SUCCESS;
}

# ifdef UNIV_DEBUG
/** Gets the desired bits for a given page from a bitmap page.
@param[in]	page		bitmap page
@param[in]	page_id		page id whose bits to get
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	bit		IBUF_BITMAP_FREE, IBUF_BITMAP_BUFFERED, ...
@param[in,out]	mtr		mini-transaction holding an x-latch on the
bitmap page
@return value of bits */
#  define ibuf_bitmap_page_get_bits(page, page_id, zip_size, bit, mtr)	\
	ibuf_bitmap_page_get_bits_low(page, page_id, zip_size,		\
				      MTR_MEMO_PAGE_X_FIX, mtr, bit)
# else /* UNIV_DEBUG */
/** Gets the desired bits for a given page from a bitmap page.
@param[in]	page		bitmap page
@param[in]	page_id		page id whose bits to get
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	bit		IBUF_BITMAP_FREE, IBUF_BITMAP_BUFFERED, ...
@param[in,out]	mtr		mini-transaction holding an x-latch on the
bitmap page
@return value of bits */
#  define ibuf_bitmap_page_get_bits(page, page_id, zip_size, bit, mtr)	\
	ibuf_bitmap_page_get_bits_low(page, page_id, zip_size, bit)
# endif /* UNIV_DEBUG */

/** Gets the desired bits for a given page from a bitmap page.
@param[in]	page		bitmap page
@param[in]	page_id		page id whose bits to get
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in]	latch_type	MTR_MEMO_PAGE_X_FIX, MTR_MEMO_BUF_FIX, ...
@param[in,out]	mtr		mini-transaction holding latch_type on the
bitmap page
@param[in]	bit		IBUF_BITMAP_FREE, IBUF_BITMAP_BUFFERED, ...
@return value of bits */
UNIV_INLINE
ulint
ibuf_bitmap_page_get_bits_low(
	const page_t*		page,
	const page_id_t		page_id,
	ulint			zip_size,
#ifdef UNIV_DEBUG
	ulint			latch_type,
	mtr_t*			mtr,
#endif /* UNIV_DEBUG */
	ulint			bit)
{
	ulint	byte_offset;
	ulint	bit_offset;
	ulint	map_byte;
	ulint	value;
	const ulint size = zip_size ? zip_size : srv_page_size;

	ut_ad(ut_is_2pow(zip_size));
	ut_ad(bit < IBUF_BITS_PER_PAGE);
	compile_time_assert(!(IBUF_BITS_PER_PAGE % 2));
	ut_ad(mtr->memo_contains_page_flagged(page, latch_type));

	bit_offset = (page_id.page_no() & (size - 1))
		* IBUF_BITS_PER_PAGE + bit;

	byte_offset = bit_offset / 8;
	bit_offset = bit_offset % 8;

	ut_ad(byte_offset + IBUF_BITMAP < srv_page_size);

	map_byte = mach_read_from_1(page + IBUF_BITMAP + byte_offset);

	value = ut_bit_get_nth(map_byte, bit_offset);

	if (bit == IBUF_BITMAP_FREE) {
		ut_ad(bit_offset + 1 < 8);

		value = value * 2 + ut_bit_get_nth(map_byte, bit_offset + 1);
	}

	return(value);
}

/** Calculates the bitmap page number for a given page number.
@param[in]	page_id		page id
@param[in]	size		page size
@return the bitmap page id where the file page is mapped */
inline page_id_t ibuf_bitmap_page_no_calc(const page_id_t page_id, ulint size)
{
  if (!size)
    size= srv_page_size;

  return page_id_t(page_id.space(), FSP_IBUF_BITMAP_OFFSET
		   + uint32_t(page_id.page_no() & ~(size - 1)));
}

/** Gets the ibuf bitmap page where the bits describing a given file page are
stored.
@param[in]	page_id		page id of the file page
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@param[in,out]	mtr		mini-transaction
@return bitmap page where the file page is mapped, that is, the bitmap
page containing the descriptor bits for the file page; the bitmap page
is x-latched */
static
buf_block_t*
ibuf_bitmap_get_map_page(
	const page_id_t		page_id,
	ulint			zip_size,
	mtr_t*			mtr)
{
  return buf_page_get_gen(ibuf_bitmap_page_no_calc(page_id, zip_size),
                          zip_size, RW_X_LATCH, nullptr,
                          BUF_GET_POSSIBLY_FREED, mtr);
}

/** Checks if a page address is an ibuf bitmap page (level 3 page) address.
@param[in]	page_id		page id
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@return TRUE if a bitmap page */
inline bool ibuf_bitmap_page(const page_id_t page_id, ulint zip_size)
{
  ut_ad(ut_is_2pow(zip_size));
  ulint size= zip_size ? zip_size : srv_page_size;
  return (page_id.page_no() & (size - 1)) == FSP_IBUF_BITMAP_OFFSET;
}

/** Returns TRUE if the page is one of the fixed address ibuf pages.
@param[in]	page_id		page id
@param[in]	zip_size	ROW_FORMAT=COMPRESSED page size, or 0
@return TRUE if a fixed address ibuf i/o page */
inline bool ibuf_fixed_addr_page(const page_id_t page_id, ulint zip_size)
{
  return page_id == ibuf_root || ibuf_bitmap_page(page_id, zip_size);
}

#ifdef UNIV_DEBUG
# define ibuf_rec_get_page_no(mtr,rec) ibuf_rec_get_page_no_func(mtr,rec)
#else /* UNIV_DEBUG */
# define ibuf_rec_get_page_no(mtr,rec) ibuf_rec_get_page_no_func(rec)
#endif /* UNIV_DEBUG */

/********************************************************************//**
Returns the page number field of an ibuf record.
@return page number */
static
uint32_t
ibuf_rec_get_page_no_func(
/*======================*/
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction owning rec */
#endif /* UNIV_DEBUG */
	const rec_t*	rec)	/*!< in: ibuf record */
{
	const byte*	field;
	ulint		len;

	ut_ad(mtr->memo_contains_page_flagged(rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));
	ut_ad(rec_get_n_fields_old(rec) > 2);

	field = rec_get_nth_field_old(rec, IBUF_REC_FIELD_MARKER, &len);

	ut_a(len == 1);

	field = rec_get_nth_field_old(rec, IBUF_REC_FIELD_PAGE, &len);

	ut_a(len == 4);

	return(mach_read_from_4(field));
}

#ifdef UNIV_DEBUG
# define ibuf_rec_get_space(mtr,rec) ibuf_rec_get_space_func(mtr,rec)
#else /* UNIV_DEBUG */
# define ibuf_rec_get_space(mtr,rec) ibuf_rec_get_space_func(rec)
#endif /* UNIV_DEBUG */

/********************************************************************//**
Returns the space id field of an ibuf record. For < 4.1.x format records
returns 0.
@return space id */
static
uint32_t
ibuf_rec_get_space_func(
/*====================*/
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction owning rec */
#endif /* UNIV_DEBUG */
	const rec_t*	rec)	/*!< in: ibuf record */
{
	const byte*	field;
	ulint		len;

	ut_ad(mtr->memo_contains_page_flagged(rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));
	ut_ad(rec_get_n_fields_old(rec) > 2);

	field = rec_get_nth_field_old(rec, IBUF_REC_FIELD_MARKER, &len);

	ut_a(len == 1);

	field = rec_get_nth_field_old(rec, IBUF_REC_FIELD_SPACE, &len);

	ut_a(len == 4);

	return(mach_read_from_4(field));
}

#ifdef UNIV_DEBUG
# define ibuf_rec_get_info(mtr,rec,op,comp,info_len,counter)	\
	ibuf_rec_get_info_func(mtr,rec,op,comp,info_len,counter)
#else /* UNIV_DEBUG */
# define ibuf_rec_get_info(mtr,rec,op,comp,info_len,counter)	\
	ibuf_rec_get_info_func(rec,op,comp,info_len,counter)
#endif
/****************************************************************//**
Get various information about an ibuf record in >= 4.1.x format. */
static
void
ibuf_rec_get_info_func(
/*===================*/
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction owning rec */
#endif /* UNIV_DEBUG */
	const rec_t*	rec,		/*!< in: ibuf record */
	ibuf_op_t*	op,		/*!< out: operation type, or NULL */
	ibool*		comp,		/*!< out: compact flag, or NULL */
	ulint*		info_len,	/*!< out: length of info fields at the
					start of the fourth field, or
					NULL */
	ulint*		counter)	/*!< in: counter value, or NULL */
{
	const byte*	types;
	ulint		fields;
	ulint		len;

	/* Local variables to shadow arguments. */
	ibuf_op_t	op_local;
	ibool		comp_local;
	ulint		info_len_local;
	ulint		counter_local;

	ut_ad(mtr->memo_contains_page_flagged(rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));
	fields = rec_get_n_fields_old(rec);
	ut_a(fields > IBUF_REC_FIELD_USER);

	types = rec_get_nth_field_old(rec, IBUF_REC_FIELD_METADATA, &len);

	info_len_local = len % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE;
	compile_time_assert(IBUF_REC_INFO_SIZE
			    < DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

	switch (info_len_local) {
	case 0:
	case 1:
		op_local = IBUF_OP_INSERT;
		comp_local = info_len_local;
		ut_ad(!counter);
		counter_local = ULINT_UNDEFINED;
		break;

	case IBUF_REC_INFO_SIZE:
		op_local = (ibuf_op_t) types[IBUF_REC_OFFSET_TYPE];
		comp_local = types[IBUF_REC_OFFSET_FLAGS] & IBUF_REC_COMPACT;
		counter_local = mach_read_from_2(
			types + IBUF_REC_OFFSET_COUNTER);
		break;

	default:
		ut_error;
	}

	ut_a(op_local < IBUF_OP_COUNT);
	ut_a((len - info_len_local) ==
	     (fields - IBUF_REC_FIELD_USER)
	     * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

	if (op) {
		*op = op_local;
	}

	if (comp) {
		*comp = comp_local;
	}

	if (info_len) {
		*info_len = info_len_local;
	}

	if (counter) {
		*counter = counter_local;
	}
}

#ifdef UNIV_DEBUG
# define ibuf_rec_get_op_type(mtr,rec) ibuf_rec_get_op_type_func(mtr,rec)
#else /* UNIV_DEBUG */
# define ibuf_rec_get_op_type(mtr,rec) ibuf_rec_get_op_type_func(rec)
#endif

/****************************************************************//**
Returns the operation type field of an ibuf record.
@return operation type */
static
ibuf_op_t
ibuf_rec_get_op_type_func(
/*======================*/
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction owning rec */
#endif /* UNIV_DEBUG */
	const rec_t*	rec)	/*!< in: ibuf record */
{
	ulint		len;

	ut_ad(mtr->memo_contains_page_flagged(rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));
	ut_ad(rec_get_n_fields_old(rec) > 2);

	(void) rec_get_nth_field_old(rec, IBUF_REC_FIELD_MARKER, &len);

	if (len > 1) {
		/* This is a < 4.1.x format record */

		return(IBUF_OP_INSERT);
	} else {
		ibuf_op_t	op;

		ibuf_rec_get_info(mtr, rec, &op, NULL, NULL, NULL);

		return(op);
	}
}

/********************************************************************//**
Creates a dummy index for inserting a record to a non-clustered index.
@return dummy index */
static
dict_index_t*
ibuf_dummy_index_create(
/*====================*/
	ulint		n,	/*!< in: number of fields */
	ibool		comp)	/*!< in: TRUE=use compact record format */
{
	dict_table_t*	table;
	dict_index_t*	index;

	table = dict_table_t::create({C_STRING_WITH_LEN("IBUF_DUMMY")},
				     nullptr, n, 0,
				     comp ? DICT_TF_COMPACT : 0, 0);

	index = dict_mem_index_create(table, "IBUF_DUMMY", 0, n);

	/* avoid ut_ad(index->cached) in dict_index_get_n_unique_in_tree */
	index->cached = TRUE;
	ut_d(index->is_dummy = true);

	return(index);
}
/********************************************************************//**
Add a column to the dummy index */
static
void
ibuf_dummy_index_add_col(
/*=====================*/
	dict_index_t*	index,	/*!< in: dummy index */
	const dtype_t*	type,	/*!< in: the data type of the column */
	ulint		len)	/*!< in: length of the column */
{
	ulint	i	= index->table->n_def;
	dict_mem_table_add_col(index->table, NULL, NULL,
			       dtype_get_mtype(type),
			       dtype_get_prtype(type),
			       dtype_get_len(type));
	dict_index_add_col(index, index->table,
			   dict_table_get_nth_col(index->table, i), len);
}
/********************************************************************//**
Deallocates a dummy index for inserting a record to a non-clustered index. */
static
void
ibuf_dummy_index_free(
/*==================*/
	dict_index_t*	index)	/*!< in, own: dummy index */
{
	dict_table_t*	table = index->table;

	dict_mem_index_free(index);
	dict_mem_table_free(table);
}

#ifdef UNIV_DEBUG
# define ibuf_build_entry_from_ibuf_rec(mtr,ibuf_rec,heap,pindex)	\
	ibuf_build_entry_from_ibuf_rec_func(mtr,ibuf_rec,heap,pindex)
#else /* UNIV_DEBUG */
# define ibuf_build_entry_from_ibuf_rec(mtr,ibuf_rec,heap,pindex)	\
	ibuf_build_entry_from_ibuf_rec_func(ibuf_rec,heap,pindex)
#endif

/*********************************************************************//**
Builds the entry used to

1) IBUF_OP_INSERT: insert into a non-clustered index

2) IBUF_OP_DELETE_MARK: find the record whose delete-mark flag we need to
   activate

3) IBUF_OP_DELETE: find the record we need to delete

when we have the corresponding record in an ibuf index.

NOTE that as we copy pointers to fields in ibuf_rec, the caller must
hold a latch to the ibuf_rec page as long as the entry is used!

@return own: entry to insert to a non-clustered index */
static
dtuple_t*
ibuf_build_entry_from_ibuf_rec_func(
/*================================*/
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction owning rec */
#endif /* UNIV_DEBUG */
	const rec_t*	ibuf_rec,	/*!< in: record in an insert buffer */
	mem_heap_t*	heap,		/*!< in: heap where built */
	dict_index_t**	pindex)		/*!< out, own: dummy index that
					describes the entry */
{
	dtuple_t*	tuple;
	dfield_t*	field;
	ulint		n_fields;
	const byte*	types;
	const byte*	data;
	ulint		len;
	ulint		info_len;
	ulint		i;
	ulint		comp;
	dict_index_t*	index;

	ut_ad(mtr->memo_contains_page_flagged(ibuf_rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));

	data = rec_get_nth_field_old(ibuf_rec, IBUF_REC_FIELD_MARKER, &len);

	ut_a(len == 1);
	ut_a(*data == 0);
	ut_a(rec_get_n_fields_old(ibuf_rec) > IBUF_REC_FIELD_USER);

	n_fields = rec_get_n_fields_old(ibuf_rec) - IBUF_REC_FIELD_USER;

	tuple = dtuple_create(heap, n_fields);

	types = rec_get_nth_field_old(ibuf_rec, IBUF_REC_FIELD_METADATA, &len);

	ibuf_rec_get_info(mtr, ibuf_rec, NULL, &comp, &info_len, NULL);

	index = ibuf_dummy_index_create(n_fields, comp);

	len -= info_len;
	types += info_len;

	ut_a(len == n_fields * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

	for (i = 0; i < n_fields; i++) {
		field = dtuple_get_nth_field(tuple, i);

		data = rec_get_nth_field_old(
			ibuf_rec, i + IBUF_REC_FIELD_USER, &len);

		dfield_set_data(field, data, len);

		dtype_new_read_for_order_and_null_size(
			dfield_get_type(field),
			types + i * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

		ibuf_dummy_index_add_col(index, dfield_get_type(field), len);
	}

	index->n_core_null_bytes = static_cast<uint8_t>(
		UT_BITS_IN_BYTES(unsigned(index->n_nullable)));

	/* Prevent an ut_ad() failure in page_zip_write_rec() by
	adding system columns to the dummy table pointed to by the
	dummy secondary index.  The insert buffer is only used for
	secondary indexes, whose records never contain any system
	columns, such as DB_TRX_ID. */
	ut_d(dict_table_add_system_columns(index->table, index->table->heap));

	*pindex = index;

	return(tuple);
}

/******************************************************************//**
Get the data size.
@return size of fields */
UNIV_INLINE
ulint
ibuf_rec_get_size(
/*==============*/
	const rec_t*	rec,			/*!< in: ibuf record */
	const byte*	types,			/*!< in: fields */
	ulint		n_fields,		/*!< in: number of fields */
	ulint		comp)			/*!< in: 0=ROW_FORMAT=REDUNDANT,
						nonzero=ROW_FORMAT=COMPACT */
{
	ulint	i;
	ulint	field_offset;
	ulint	types_offset;
	ulint	size = 0;

	field_offset = IBUF_REC_FIELD_USER;
	types_offset = DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE;

	for (i = 0; i < n_fields; i++) {
		ulint		len;
		dtype_t		dtype;

		rec_get_nth_field_offs_old(rec, i + field_offset, &len);

		if (len != UNIV_SQL_NULL) {
			size += len;
		} else {
			dtype_new_read_for_order_and_null_size(&dtype, types);

			size += dtype_get_sql_null_size(&dtype, comp);
		}

		types += types_offset;
	}

	return(size);
}

/*********************************************************************//**
Builds a search tuple used to search buffered inserts for an index page.
This is for >= 4.1.x format records.
@return own: search tuple */
static
dtuple_t*
ibuf_search_tuple_build(
/*====================*/
	ulint		space,	/*!< in: space id */
	ulint		page_no,/*!< in: index page number */
	mem_heap_t*	heap)	/*!< in: heap into which to build */
{
	dtuple_t*	tuple;
	dfield_t*	field;
	byte*		buf;

	tuple = dtuple_create(heap, IBUF_REC_FIELD_METADATA);

	/* Store the space id in tuple */

	field = dtuple_get_nth_field(tuple, IBUF_REC_FIELD_SPACE);

	buf = static_cast<byte*>(mem_heap_alloc(heap, 4));

	mach_write_to_4(buf, space);

	dfield_set_data(field, buf, 4);

	/* Store the new format record marker byte */

	field = dtuple_get_nth_field(tuple, IBUF_REC_FIELD_MARKER);

	buf = static_cast<byte*>(mem_heap_alloc(heap, 1));

	mach_write_to_1(buf, 0);

	dfield_set_data(field, buf, 1);

	/* Store the page number in tuple */

	field = dtuple_get_nth_field(tuple, IBUF_REC_FIELD_PAGE);

	buf = static_cast<byte*>(mem_heap_alloc(heap, 4));

	mach_write_to_4(buf, page_no);

	dfield_set_data(field, buf, 4);

	dtuple_set_types_binary(tuple, IBUF_REC_FIELD_METADATA);

	return(tuple);
}

/*********************************************************************//**
Removes a page from the free list and frees it to the fsp system. */
void ibuf_remove_free_page() // FIXME: ibuf_free_excess_pages() on startup!
{
	mtr_t	mtr;
	mtr_t	mtr2;

	log_free_check();

	mtr_start(&mtr);
	/* Acquire the fsp latch before the ibuf header, obeying the latching
	order */

	mtr.x_lock_space(fil_system.sys_space);
        buf_block_t* header =
		buf_page_get(page_id_t(0, FSP_IBUF_HEADER_PAGE_NO),
                             0, RW_X_LATCH, &mtr);

	if (!header) {
early_exit:
		mtr.commit();
		return;
	}

	mtr2.start();
	mtr_sx_lock_index(ibuf_index, &mtr2);
	buf_block_t* root = buf_page_get_gen(ibuf_root, 0, RW_SX_LATCH,
					     nullptr, BUF_GET, &mtr2);

	if (UNIV_UNLIKELY(!root)) {
		mtr2.commit();
		goto early_exit;
	}

	const uint32_t page_no = flst_get_last(PAGE_HEADER
					       + PAGE_BTR_IBUF_FREE_LIST
					       + root->page.frame).page;

	/* NOTE that we must release the latch on the ibuf tree root
	because in fseg_free_page we access level 1 pages, and the root
	is a level 2 page. */

	mtr2.commit();

	/* Since pessimistic inserts were prevented, we know that the
	page is still in the free list. NOTE that also deletes may take
	pages from the free list, but they take them from the start, and
	the free list was so long that they cannot have taken the last
	page from it. */

	const page_id_t	page_id{0, page_no};
	dberr_t err = fseg_free_page(header->page.frame + PAGE_DATA,
				     fil_system.sys_space, page_no, &mtr);

	if (err != DB_SUCCESS) {
		goto func_exit;
	}

	mtr_sx_lock_index(ibuf_index, &mtr);

	root = buf_page_get_gen(ibuf_root, 0, RW_SX_LATCH, nullptr, BUF_GET,
				&mtr);

	if (UNIV_UNLIKELY(!root)) {
		goto func_exit;
	}

	ut_ad(page_no == flst_get_last(PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST
				       + root->page.frame).page);

	/* Remove the page from the free list and update the ibuf size data */
	if (buf_block_t* block =
	    buf_page_get_gen(page_id, 0, RW_X_LATCH, nullptr, BUF_GET,
			     &mtr, &err)) {
		err = flst_remove(root, PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST,
				  block,
				  PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST_NODE,
				  &mtr);
	}

func_exit:
	if (err == DB_SUCCESS) {
		buf_page_free(fil_system.sys_space, page_no, &mtr);
	}

	mtr.commit();
}

#ifdef UNIV_DEBUG
# define ibuf_get_merge_page_nos(rec,mtr,ids,pages,n_stored) \
	ibuf_get_merge_page_nos_func(rec,mtr,ids,pages,n_stored)
#else /* UNIV_DEBUG */
# define ibuf_get_merge_page_nos(rec,mtr,ids,pages,n_stored) \
	ibuf_get_merge_page_nos_func(rec,ids,pages,n_stored)
#endif /* UNIV_DEBUG */

/*********************************************************************//**
Reads page numbers from a leaf in an ibuf tree. */
static
void
ibuf_get_merge_page_nos_func(
/*=========================*/
	const rec_t*	rec,	/*!< in: insert buffer record */
#ifdef UNIV_DEBUG
	mtr_t*		mtr,	/*!< in: mini-transaction holding rec */
#endif /* UNIV_DEBUG */
	uint32_t*	space_ids,/*!< in/out: space id's of the pages */
	uint32_t*	page_nos,/*!< in/out: buffer for at least
				IBUF_MAX_N_PAGES_MERGED many page numbers;
				the page numbers are in an ascending order */
	ulint*		n_stored)/*!< out: number of page numbers stored to
				page_nos in this function */
{
	uint32_t prev_page_no;
	uint32_t prev_space_id;
	uint32_t first_page_no;
	uint32_t first_space_id;
	uint32_t rec_page_no;
	uint32_t rec_space_id;
	ulint	limit;
	ulint	n_pages;

	ut_ad(mtr->memo_contains_page_flagged(rec, MTR_MEMO_PAGE_X_FIX
					      | MTR_MEMO_PAGE_S_FIX));

	*n_stored = 0;

	if (page_rec_is_supremum(rec)) {

		rec = page_rec_get_prev_const(rec);
		if (UNIV_UNLIKELY(!rec)) {
corruption:
			ut_ad("corrupted page" == 0);
			return;
		}
	}

	if (page_rec_is_infimum(rec)) {
		rec = page_rec_get_next_const(rec);
		if (!rec || page_rec_is_supremum(rec)) {
			return;
		}
	}

	limit = ut_min(IBUF_MAX_N_PAGES_MERGED,
		       buf_pool_get_curr_size() / 4);

	first_page_no = ibuf_rec_get_page_no(mtr, rec);
	first_space_id = ibuf_rec_get_space(mtr, rec);
	n_pages = 0;
	prev_page_no = 0;
	prev_space_id = 0;

	/* Go backwards from the first rec until we reach the border of the
	'merge area', or the page start or the limit of storeable pages is
	reached */

	while (!page_rec_is_infimum(rec) && UNIV_LIKELY(n_pages < limit)) {

		rec_page_no = ibuf_rec_get_page_no(mtr, rec);
		rec_space_id = ibuf_rec_get_space(mtr, rec);

		if (rec_space_id != first_space_id
		    || (rec_page_no / IBUF_MERGE_AREA)
		    != (first_page_no / IBUF_MERGE_AREA)) {

			break;
		}

		if (rec_page_no != prev_page_no
		    || rec_space_id != prev_space_id) {
			n_pages++;
		}

		prev_page_no = rec_page_no;
		prev_space_id = rec_space_id;

		if (UNIV_UNLIKELY(!(rec = page_rec_get_prev_const(rec)))) {
			goto corruption;
		}
	}

	rec = page_rec_get_next_const(rec);

	/* At the loop start there is no prev page; we mark this with a pair
	of space id, page no (0, 0) for which there can never be entries in
	the insert buffer */

	prev_page_no = 0;
	prev_space_id = 0;

	while (*n_stored < limit && rec) {
		if (page_rec_is_supremum(rec)) {
			/* When no more records available, mark this with
			another 'impossible' pair of space id, page no */
			rec_page_no = 1;
			rec_space_id = 0;
		} else {
			rec_page_no = ibuf_rec_get_page_no(mtr, rec);
			rec_space_id = ibuf_rec_get_space(mtr, rec);
			/* In the system tablespace the smallest
			possible secondary index leaf page number is
			bigger than FSP_DICT_HDR_PAGE_NO (7).
			In all tablespaces, pages 0 and 1 are reserved
			for the allocation bitmap and the change
			buffer bitmap. In file-per-table tablespaces,
			a file segment inode page will be created at
			page 2 and the clustered index tree is created
			at page 3.  So for file-per-table tablespaces,
			page 4 is the smallest possible secondary
			index leaf page. CREATE TABLESPACE also initially
			uses pages 2 and 3 for the first created table,
			but that table may be dropped, allowing page 2
			to be reused for a secondary index leaf page.
			To keep this assertion simple, just
			make sure the page is >= 2. */
			ut_ad(rec_page_no >= FSP_FIRST_INODE_PAGE_NO);
		}

#ifdef UNIV_IBUF_DEBUG
		ut_a(*n_stored < IBUF_MAX_N_PAGES_MERGED);
#endif
		if ((rec_space_id != prev_space_id
		     || rec_page_no != prev_page_no)
		    && (prev_space_id != 0 || prev_page_no != 0)) {

			space_ids[*n_stored] = prev_space_id;
			page_nos[*n_stored] = prev_page_no;
			(*n_stored)++;

			if (rec_space_id != first_space_id
			    || rec_page_no / IBUF_MERGE_AREA
			    != first_page_no / IBUF_MERGE_AREA) {

				break;
			}
		}

		if (rec_page_no == 1 && rec_space_id == 0) {
			/* Supremum record */

			break;
		}

		prev_page_no = rec_page_no;
		prev_space_id = rec_space_id;

		rec = page_rec_get_next_const(rec);
	}

#ifdef UNIV_IBUF_DEBUG
	ut_a(*n_stored <= IBUF_MAX_N_PAGES_MERGED);
#endif
}

/*******************************************************************//**
Get the matching records for space id.
@return current rec or NULL */
static	MY_ATTRIBUTE((nonnull, warn_unused_result))
const rec_t*
ibuf_get_user_rec(
/*===============*/
	btr_pcur_t*	pcur,		/*!< in: the current cursor */
	mtr_t*		mtr)		/*!< in: mini transaction */
{
	do {
		const rec_t* rec = btr_pcur_get_rec(pcur);

		if (page_rec_is_user_rec(rec)) {
			return(rec);
		}
	} while (btr_pcur_move_to_next(pcur, mtr));

	return(NULL);
}

/*********************************************************************//**
Reads page numbers for a space id from an ibuf tree. */
MY_ATTRIBUTE((nonnull)) // FIXME: use this
void
ibuf_get_merge_pages(
/*=================*/
	btr_pcur_t*	pcur,	/*!< in/out: cursor */
	uint32_t	space,	/*!< in: space for which to merge */
	ulint		limit,	/*!< in: max page numbers to read */
	uint32_t*	pages,	/*!< out: pages read */
	uint32_t*	spaces,	/*!< out: spaces read */
	ulint*		n_pages,/*!< out: number of pages read */
	mtr_t*		mtr)	/*!< in: mini transaction */
{
	const rec_t*	rec;

	*n_pages = 0;

	while ((rec = ibuf_get_user_rec(pcur, mtr)) != 0
	       && ibuf_rec_get_space(mtr, rec) == space
	       && *n_pages < limit) {

		uint32_t page_no = ibuf_rec_get_page_no(mtr, rec);

		if (*n_pages == 0 || pages[*n_pages - 1] != page_no) {
			spaces[*n_pages] = space;
			pages[*n_pages] = page_no;
			++*n_pages;
		}

		btr_pcur_move_to_next(pcur, mtr);
	}
}

/**
Delete a change buffer record.
@param[in]	page_id		page identifier
@param[in,out]	pcur		persistent cursor positioned on the record
@param[in]	search_tuple	search key for (space,page_no)
@param[in,out]	mtr		mini-transaction
@return whether mtr was committed (due to pessimistic operation) */
static MY_ATTRIBUTE((warn_unused_result, nonnull))
bool ibuf_delete_rec(const page_id_t page_id, btr_pcur_t* pcur,
		     const dtuple_t* search_tuple, mtr_t* mtr);

/** Delete the change buffer records for the given page id
@param page_id page identifier */
static void ibuf_delete_recs(const page_id_t page_id)
{
  dfield_t dfield[IBUF_REC_FIELD_METADATA];
  dtuple_t tuple {0,IBUF_REC_FIELD_METADATA,IBUF_REC_FIELD_METADATA,
                  dfield,0,nullptr
#ifdef UNIV_DEBUG
                  ,DATA_TUPLE_MAGIC_N
#endif
  };
  byte space_id[4], page_no[4];

  mach_write_to_4(space_id, page_id.space());
  mach_write_to_4(page_no, page_id.page_no());

  dfield_set_data(&dfield[0], space_id, 4);
  dfield_set_data(&dfield[1], field_ref_zero, 1);
  dfield_set_data(&dfield[2], page_no, 4);
  dtuple_set_types_binary(&tuple, IBUF_REC_FIELD_METADATA);

  mtr_t mtr;
loop:
  btr_pcur_t pcur;
  pcur.btr_cur.page_cur.index= ibuf_index;
  mtr.start();
  if (btr_pcur_open(&tuple, PAGE_CUR_GE, BTR_MODIFY_LEAF, &pcur, 0, &mtr))
    goto func_exit;
  if (!btr_pcur_is_on_user_rec(&pcur))
  {
    ut_ad(btr_pcur_is_after_last_on_page(&pcur));
    goto func_exit;
  }

  for (;;)
  {
    ut_ad(btr_pcur_is_on_user_rec(&pcur));
    const rec_t* ibuf_rec = btr_pcur_get_rec(&pcur);
    if (ibuf_rec_get_space(&mtr, ibuf_rec) != page_id.space()
        || ibuf_rec_get_page_no(&mtr, ibuf_rec) != page_id.page_no())
      break;
    /* Delete the record from ibuf */
    if (ibuf_delete_rec(page_id, &pcur, &tuple, &mtr))
    {
      /* Deletion was pessimistic and mtr was committed:
      we start from the beginning again */
      ut_ad(mtr.has_committed());
      goto loop;
    }

    if (btr_pcur_is_after_last_on_page(&pcur))
    {
      mtr.commit();
      btr_pcur_close(&pcur);
      goto loop;
    }
  }
func_exit:
  mtr.commit();
  btr_pcur_close(&pcur);
}

MY_ATTRIBUTE((nonnull, warn_unused_result))
/********************************************************************//**
During merge, inserts to an index page a secondary index entry extracted
from the insert buffer.
@return	error code */
static
dberr_t
ibuf_insert_to_index_page_low(
/*==========================*/
	const dtuple_t*	entry,	/*!< in: buffered entry to insert */
	rec_offs**	offsets,/*!< out: offsets on *rec */
	mem_heap_t*	heap,	/*!< in/out: memory heap */
	mtr_t*		mtr,	/*!< in/out: mtr */
	page_cur_t*	page_cur)/*!< in/out: cursor positioned on the record
				after which to insert the buffered entry */
{
  if (page_cur_tuple_insert(page_cur, entry, offsets, &heap, 0, mtr))
    return DB_SUCCESS;

  /* Page reorganization or recompression should already have been
  attempted by page_cur_tuple_insert(). */
  ut_ad(!is_buf_block_get_page_zip(page_cur->block));

  /* If the record did not fit, reorganize */
  if (dberr_t err= btr_page_reorganize(page_cur, mtr))
    return err;

  /* This time the record must fit */
  if (page_cur_tuple_insert(page_cur, entry, offsets, &heap, 0, mtr))
    return DB_SUCCESS;

  return DB_CORRUPTION;
}

/************************************************************************
During merge, inserts to an index page a secondary index entry extracted
from the insert buffer. */
static
dberr_t
ibuf_insert_to_index_page(
/*======================*/
	const dtuple_t*	entry,	/*!< in: buffered entry to insert */
	buf_block_t*	block,	/*!< in/out: index page where the buffered entry
				should be placed */
	dict_index_t*	index,	/*!< in: record descriptor */
	mtr_t*		mtr)	/*!< in: mtr */
{
	page_cur_t	page_cur;
	page_t*		page		= buf_block_get_frame(block);
	rec_t*		rec;
	rec_offs*	offsets;
	mem_heap_t*	heap;

	DBUG_PRINT("ibuf", ("page " UINT32PF ":" UINT32PF,
			    block->page.id().space(),
			    block->page.id().page_no()));

	ut_ad(!dict_index_is_online_ddl(index));// this is an ibuf_dummy index
	ut_ad(dtuple_check_typed(entry));
#ifdef BTR_CUR_HASH_ADAPT
	/* ibuf_cleanup() must finish before the adaptive hash index
	can be inserted into. */
	ut_ad(!block->index);
#endif /* BTR_CUR_HASH_ADAPT */
	ut_ad(mtr->is_named_space(block->page.id().space()));

	if (UNIV_UNLIKELY(dict_table_is_comp(index->table)
			  != (ibool)!!page_is_comp(page))) {
		return DB_CORRUPTION;
	}

	rec = page_rec_get_next(page_get_infimum_rec(page));

	if (!rec || page_rec_is_supremum(rec)) {
		return DB_CORRUPTION;
	}

	if (!rec_n_fields_is_sane(index, rec, entry)) {
		return DB_CORRUPTION;
	}

	ulint up_match = 0, low_match = 0;
	page_cur.index = index;
	page_cur.block = block;

	if (page_cur_search_with_match(entry, PAGE_CUR_LE,
				       &up_match, &low_match, &page_cur,
				       nullptr)) {
		return DB_CORRUPTION;
	}

	dberr_t err = DB_SUCCESS;

	heap = mem_heap_create(
		sizeof(upd_t)
		+ REC_OFFS_HEADER_SIZE * sizeof(*offsets)
		+ dtuple_get_n_fields(entry)
		* (sizeof(upd_field_t) + sizeof *offsets));

	if (UNIV_UNLIKELY(low_match == dtuple_get_n_fields(entry))) {
		upd_t*		update;

		rec = page_cur_get_rec(&page_cur);

		/* This is based on
		row_ins_sec_index_entry_by_modify(BTR_MODIFY_LEAF). */
		ut_ad(rec_get_deleted_flag(rec, page_is_comp(page)));

		offsets = rec_get_offsets(rec, index, NULL, index->n_fields,
					  ULINT_UNDEFINED, &heap);
		update = row_upd_build_sec_rec_difference_binary(
			rec, index, offsets, entry, heap);

		if (update->n_fields == 0) {
			/* The records only differ in the delete-mark.
			Clear the delete-mark, like we did before
			Bug #56680 was fixed. */
			btr_rec_set_deleted<false>(block, rec, mtr);
			goto updated_in_place;
		}

		/* Copy the info bits. Clear the delete-mark. */
		update->info_bits = rec_get_info_bits(rec, page_is_comp(page));
		update->info_bits &= byte(~REC_INFO_DELETED_FLAG);
		page_zip_des_t* page_zip = buf_block_get_page_zip(block);

		/* We cannot invoke btr_cur_optimistic_update() here,
		because we do not have a btr_cur_t or que_thr_t,
		as the insert buffer merge occurs at a very low level. */
		if (!row_upd_changes_field_size_or_external(index, offsets,
							    update)
		    && (!page_zip || btr_cur_update_alloc_zip(
				page_zip, &page_cur, offsets,
				rec_offs_size(offsets), false, mtr))) {
			/* This is the easy case. Do something similar
			to btr_cur_update_in_place(). */
			rec = page_cur_get_rec(&page_cur);
			btr_cur_upd_rec_in_place(rec, index, offsets,
						 update, block, mtr);

			DBUG_EXECUTE_IF(
				"crash_after_log_ibuf_upd_inplace",
				log_buffer_flush_to_disk();
				ib::info() << "Wrote log record for ibuf"
					" update in place operation";
				DBUG_SUICIDE();
			);

			goto updated_in_place;
		}

		/* btr_cur_update_alloc_zip() may have changed this */
		rec = page_cur_get_rec(&page_cur);

		/* A collation may identify values that differ in
		storage length.
		Some examples (1 or 2 bytes):
		utf8_turkish_ci: I = U+0131 LATIN SMALL LETTER DOTLESS I
		utf8_general_ci: S = U+00DF LATIN SMALL LETTER SHARP S
		utf8_general_ci: A = U+00E4 LATIN SMALL LETTER A WITH DIAERESIS

		latin1_german2_ci: SS = U+00DF LATIN SMALL LETTER SHARP S

		Examples of a character (3-byte UTF-8 sequence)
		identified with 2 or 4 characters (1-byte UTF-8 sequences):

		utf8_unicode_ci: 'II' = U+2171 SMALL ROMAN NUMERAL TWO
		utf8_unicode_ci: '(10)' = U+247D PARENTHESIZED NUMBER TEN
		*/

		/* Delete the different-length record, and insert the
		buffered one. */

		page_cur_delete_rec(&page_cur, offsets, mtr);
		if (!(page_cur_move_to_prev(&page_cur))) {
			err = DB_CORRUPTION;
			goto updated_in_place;
		}
	} else {
		offsets = NULL;
	}

	err = ibuf_insert_to_index_page_low(entry, &offsets, heap, mtr,
                                            &page_cur);
updated_in_place:
	mem_heap_free(heap);

	return err;
}

/****************************************************************//**
During merge, sets the delete mark on a record for a secondary index
entry. */
static
void
ibuf_set_del_mark(
/*==============*/
	const dtuple_t*		entry,	/*!< in: entry */
	buf_block_t*		block,	/*!< in/out: block */
	dict_index_t*		index,	/*!< in: record descriptor */
	mtr_t*			mtr)	/*!< in: mtr */
{
	page_cur_t	page_cur;
	page_cur.block = block;
	page_cur.index = index;
	ulint		up_match = 0, low_match = 0;

	ut_ad(dtuple_check_typed(entry));

	if (!page_cur_search_with_match(entry, PAGE_CUR_LE,
					&up_match, &low_match, &page_cur,
					nullptr)
	    && low_match == dtuple_get_n_fields(entry)) {
		rec_t* rec = page_cur_get_rec(&page_cur);

		/* Delete mark the old index record. According to a
		comment in row_upd_sec_index_entry(), it can already
		have been delete marked if a lock wait occurred in
		row_ins_sec_index_entry() in a previous invocation of
		row_upd_sec_index_entry(). */

		if (UNIV_LIKELY
		    (!rec_get_deleted_flag(
			    rec, dict_table_is_comp(index->table)))) {
			btr_rec_set_deleted<true>(block, rec, mtr);
		}
	} else {
		const page_t*		page
			= page_cur_get_page(&page_cur);
		const buf_block_t*	block
			= page_cur_get_block(&page_cur);

		ib::error() << "Unable to find a record to delete-mark";
		fputs("InnoDB: tuple ", stderr);
		dtuple_print(stderr, entry);
		fputs("\n"
		      "InnoDB: record ", stderr);
		rec_print(stderr, page_cur_get_rec(&page_cur), index);

		ib::error() << "page " << block->page.id() << " ("
			<< page_get_n_recs(page) << " records, index id "
			<< btr_page_get_index_id(page) << ").";

		ib::error() << BUG_REPORT_MSG;
		ut_ad(0);
	}
}

/****************************************************************//**
During merge, delete a record for a secondary index entry. */
static
void
ibuf_delete(
/*========*/
	const dtuple_t*	entry,	/*!< in: entry */
	buf_block_t*	block,	/*!< in/out: block */
	dict_index_t*	index,	/*!< in: record descriptor */
	mtr_t*		mtr)	/*!< in/out: mtr; must be committed
				before latching any further pages */
{
	page_cur_t	page_cur;
	page_cur.block = block;
	page_cur.index = index;
	ulint		up_match = 0, low_match = 0;

	ut_ad(dtuple_check_typed(entry));
	ut_ad(!index->is_spatial());
	ut_ad(!index->is_clust());

	if (!page_cur_search_with_match(entry, PAGE_CUR_LE,
					&up_match, &low_match, &page_cur,
					nullptr)
	    && low_match == dtuple_get_n_fields(entry)) {
		page_t*		page	= buf_block_get_frame(block);
		rec_t*		rec	= page_cur_get_rec(&page_cur);

		/* TODO: the below should probably be a separate function,
		it's a bastardized version of btr_cur_optimistic_delete. */

		rec_offs	offsets_[REC_OFFS_NORMAL_SIZE];
		rec_offs*	offsets	= offsets_;
		mem_heap_t*	heap = NULL;

		rec_offs_init(offsets_);

		offsets = rec_get_offsets(rec, index, offsets, index->n_fields,
					  ULINT_UNDEFINED, &heap);

		if (page_get_n_recs(page) <= 1
		    || !(REC_INFO_DELETED_FLAG
			 & rec_get_info_bits(rec, page_is_comp(page)))) {
			/* Refuse to purge the last record or a
			record that has not been marked for deletion. */
			ib::error() << "Unable to purge a record";
			fputs("InnoDB: tuple ", stderr);
			dtuple_print(stderr, entry);
			fputs("\n"
			      "InnoDB: record ", stderr);
			rec_print_new(stderr, rec, offsets);
			fprintf(stderr, "\nspace " UINT32PF " offset " UINT32PF
				" (%u records, index id %llu)\n"
				"InnoDB: Submit a detailed bug report"
				" to https://jira.mariadb.org/\n",
				block->page.id().space(),
				block->page.id().page_no(),
				(unsigned) page_get_n_recs(page),
				(ulonglong) btr_page_get_index_id(page));

			ut_ad(0);
			return;
		}

#ifdef UNIV_ZIP_DEBUG
		page_zip_des_t*	page_zip= buf_block_get_page_zip(block);
		ut_a(!page_zip || page_zip_validate(page_zip, page, index));
#endif /* UNIV_ZIP_DEBUG */
		page_cur_delete_rec(&page_cur, offsets, mtr);
#ifdef UNIV_ZIP_DEBUG
		ut_a(!page_zip || page_zip_validate(page_zip, page, index));
#endif /* UNIV_ZIP_DEBUG */

		if (UNIV_LIKELY_NULL(heap)) {
			mem_heap_free(heap);
		}
	}
}

/*********************************************************************//**
Restores insert buffer tree cursor position
@return whether the position was restored */
static MY_ATTRIBUTE((nonnull))
bool
ibuf_restore_pos(
/*=============*/
	const page_id_t	page_id,/*!< in: page identifier */
	const dtuple_t*	search_tuple,
				/*!< in: search tuple for entries of page_no */
	btr_latch_mode	mode,	/*!< in: BTR_MODIFY_LEAF or BTR_PURGE_TREE */
	btr_pcur_t*	pcur,	/*!< in/out: persistent cursor whose
				position is to be restored */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_PURGE_TREE);

	if (UNIV_LIKELY(pcur->restore_position(mode, mtr) ==
	      btr_pcur_t::SAME_ALL)) {
		return true;
	}

	if (fil_space_t* s = fil_space_t::get(page_id.space())) {
		ib::error() << "ibuf cursor restoration fails!"
			" ibuf record inserted to page "
			<< page_id
			<< " in file " << s->chain.start->name;
		s->release();

		ib::error() << BUG_REPORT_MSG;

		rec_print_old(stderr, btr_pcur_get_rec(pcur));
		rec_print_old(stderr, pcur->old_rec);
		dtuple_print(stderr, search_tuple);
	}

	btr_pcur_commit_specify_mtr(pcur, mtr);
	return false;
}

/**
Delete a change buffer record.
@param[in]	page_id		page identifier
@param[in,out]	pcur		persistent cursor positioned on the record
@param[in]	search_tuple	search key for (space,page_no)
@param[in,out]	mtr		mini-transaction
@return whether mtr was committed (due to pessimistic operation) */
static MY_ATTRIBUTE((warn_unused_result, nonnull))
bool ibuf_delete_rec(const page_id_t page_id, btr_pcur_t* pcur,
		     const dtuple_t* search_tuple, mtr_t* mtr)
{
	dberr_t		err;

	ut_ad(page_rec_is_user_rec(btr_pcur_get_rec(pcur)));
	ut_ad(ibuf_rec_get_page_no(mtr, btr_pcur_get_rec(pcur))
	      == page_id.page_no());
	ut_ad(ibuf_rec_get_space(mtr, btr_pcur_get_rec(pcur))
	      == page_id.space());

	switch (btr_cur_optimistic_delete(btr_pcur_get_btr_cur(pcur),
					  BTR_CREATE_FLAG, mtr)) {
	case DB_FAIL:
		break;
	case DB_SUCCESS:
		if (page_is_empty(btr_pcur_get_page(pcur))) {
			/* If a B-tree page is empty, it must be the root page
			and the whole B-tree must be empty. InnoDB does not
			allow empty B-tree pages other than the root. */
			ut_ad(btr_pcur_get_block(pcur)->page.id()
			      == ibuf_root);
		}
		/* fall through */
	default:
		return(FALSE);
	}

	/* We have to resort to a pessimistic delete from ibuf.
	Delete-mark the record so that it will not be applied again,
	in case the server crashes before the pessimistic delete is
	made persistent. */
	btr_rec_set_deleted<true>(btr_pcur_get_block(pcur),
				  btr_pcur_get_rec(pcur), mtr);

	btr_pcur_store_position(pcur, mtr);
	btr_pcur_commit_specify_mtr(pcur, mtr);

	mtr->start();

	if (!ibuf_restore_pos(page_id, search_tuple, BTR_PURGE_TREE,
			      pcur, mtr)) {

		ut_ad(mtr->has_committed());
		goto func_exit;
	}

	btr_cur_pessimistic_delete(&err, TRUE,
				   btr_pcur_get_btr_cur(pcur),
				   BTR_CREATE_FLAG, false, mtr);
	ut_a(err == DB_SUCCESS);
	btr_pcur_commit_specify_mtr(pcur, mtr);

func_exit:
	ut_ad(mtr->has_committed());
	btr_pcur_close(pcur);

	return(TRUE);
}

/** Reset the bits in the bitmap page for the given block and page id.
@param page_id  page identifier
@param zip_size ROW_FORMAT=COMPRESSED page size, or 0
@param mtr      mini-transaction */
static void ibuf_reset_bitmap(page_id_t page_id, ulint zip_size, mtr_t *mtr)
{
  ut_ad(mtr->is_named_space(page_id.space()));

  buf_block_t *bitmap=
    buf_page_get_gen(ibuf_bitmap_page_no_calc(page_id, zip_size),
                     zip_size, RW_X_LATCH, nullptr, BUF_GET_POSSIBLY_FREED,
                     mtr);
  if (!bitmap)
    return;

  const ulint physical_size = zip_size ? zip_size : srv_page_size;

  ulint nibble_offset= page_id.page_no() % physical_size;
  byte *map_byte= &bitmap->page.frame[IBUF_BITMAP + nibble_offset / 2];

  /* We must reset IBUF_BITMAP_BUFFERED, but at the same time we will also
  reset IBUF_BITMAP_FREE (and IBUF_BITMAP_IBUF, which should be clear). */
  byte b= *map_byte & ((nibble_offset & 1) ? byte{0xf} : byte{0xf0});
  mtr->write<1,mtr_t::MAYBE_NOP>(*bitmap, map_byte, b);
}

/** When an index page is read from a disk to the buffer pool, this function
applies any buffered operations to the page and deletes the entries from the
insert buffer. If the page is not read, but created in the buffer pool, this
function deletes its buffered entries from the insert buffer; there can
exist entries for such a page if the page belonged to an index which
subsequently was dropped.
@param block    X-latched page to try to apply changes to
@param page_id  page identifier
@param zip_size ROW_FORMAT=COMPRESSED page size, or 0
@return error code */
static dberr_t ibuf_merge_or_delete_for_page(buf_block_t *block,
					     const page_id_t page_id,
					     ulint zip_size)
{
	ut_ad(!block || page_id == block->page.id());
	ut_ad(!block || block->page.frame);
	ut_ad(!block || !block->page.is_reinit());
	ut_ad(!trx_sys_hdr_page(page_id));
	ut_ad(page_id < page_id_t(SRV_SPACE_ID_UPPER_BOUND, 0));

	const ulint physical_size = zip_size ? zip_size : srv_page_size;

	if (ibuf_fixed_addr_page(page_id, physical_size)
	    || fsp_descr_page(page_id, physical_size)) {
		return DB_SUCCESS;
	}

	btr_pcur_t	pcur;
#ifdef UNIV_IBUF_DEBUG
	ulint		volume			= 0;
#endif /* UNIV_IBUF_DEBUG */
	dberr_t		err = DB_SUCCESS;
	mtr_t		mtr;

	fil_space_t* space = fil_space_t::get(page_id.space());

	if (UNIV_UNLIKELY(!space)) {
		block = nullptr;
	} else {
		ulint	bitmap_bits = 0;

		mtr.start();

		buf_block_t* bitmap_page = ibuf_bitmap_get_map_page(
			page_id, zip_size, &mtr);

		if (bitmap_page
		    && fil_page_get_type(bitmap_page->page.frame)
		    != FIL_PAGE_TYPE_ALLOCATED) {
			bitmap_bits = ibuf_bitmap_page_get_bits(
				bitmap_page->page.frame, page_id, zip_size,
				IBUF_BITMAP_BUFFERED, &mtr);
		}

		mtr.commit();

		if (bitmap_bits
		    && DB_SUCCESS
		    == fseg_page_is_allocated(space, page_id.page_no())) {
			mtr.start();
			mtr.set_named_space(space);
			ibuf_reset_bitmap(page_id, zip_size, &mtr);
			mtr.commit();
			bitmap_bits = 0;
			if (!block
			    || btr_page_get_index_id(block->page.frame)
			    != ibuf_index_id) {
				ibuf_delete_recs(page_id);
			}
		}

		if (!bitmap_bits) {
			/* No changes are buffered for this page. */
			space->release();
			return DB_SUCCESS;
		}
	}

	if (!block) {
	} else if (!fil_page_index_page_check(block->page.frame)
		   || !page_is_leaf(block->page.frame)) {
		space->set_corrupted();
		err = DB_CORRUPTION;
		block = nullptr;
	} else {
		/* Move the ownership of the x-latch on the page to this OS
		thread, so that we can acquire a second x-latch on it. This
		is needed for the insert operations to the index page to pass
		the debug checks. */

		block->page.lock.claim_ownership();
	}

	mem_heap_t* heap = mem_heap_create(512);

	const dtuple_t* search_tuple = ibuf_search_tuple_build(
		page_id.space(), page_id.page_no(), heap);

	/* Counts for merged & discarded operations. */
	pcur.btr_cur.page_cur.index = ibuf_index;

loop:
	mtr.start();

	/* Position pcur in the insert buffer at the first entry for this
	index page */
	if (btr_pcur_open_on_user_rec(search_tuple, PAGE_CUR_GE,
				      BTR_MODIFY_LEAF, &pcur, &mtr)
	    != DB_SUCCESS) {
		err = DB_CORRUPTION;
		goto reset_bit;
	}

	if (block) {
		block->page.fix();
		block->page.lock.x_lock_recursive();
		mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);
	}

	if (space) {
		mtr.set_named_space(space);
	}

	if (!btr_pcur_is_on_user_rec(&pcur)) {
		ut_ad(btr_pcur_is_after_last_on_page(&pcur));
		goto reset_bit;
	}

	for (;;) {
		rec_t*	rec;

		ut_ad(btr_pcur_is_on_user_rec(&pcur));

		rec = btr_pcur_get_rec(&pcur);

		/* Check if the entry is for this index page */
		if (ibuf_rec_get_page_no(&mtr, rec) != page_id.page_no()
		    || ibuf_rec_get_space(&mtr, rec) != page_id.space()) {

			if (block != NULL) {
				page_header_reset_last_insert(block, &mtr);
			}

			goto reset_bit;
		}

		if (err) {
			fputs("InnoDB: Discarding record\n ", stderr);
			rec_print_old(stderr, rec);
			fputs("\nInnoDB: from the insert buffer!\n\n", stderr);
		} else if (block != NULL && !rec_get_deleted_flag(rec, 0)) {
			/* Now we have at pcur a record which should be
			applied on the index page; NOTE that the call below
			copies pointers to fields in rec, and we must
			keep the latch to the rec page until the
			insertion is finished! */
			dtuple_t*	entry;
			trx_id_t	max_trx_id;
			dict_index_t*	dummy_index;
			ibuf_op_t	op = ibuf_rec_get_op_type(&mtr, rec);

			max_trx_id = page_get_max_trx_id(page_align(rec));
			page_update_max_trx_id(block,
					       buf_block_get_page_zip(block),
					       max_trx_id, &mtr);

			ut_ad(page_validate(page_align(rec), ibuf_index));

			entry = ibuf_build_entry_from_ibuf_rec(
				&mtr, rec, heap, &dummy_index);
			ut_ad(!dummy_index->table->space);
			dummy_index->table->space = space;
			dummy_index->table->space_id = space->id;

			ut_ad(page_validate(block->page.frame, dummy_index));

			switch (op) {
			case IBUF_OP_INSERT:
#ifdef UNIV_IBUF_DEBUG
				volume += rec_get_converted_size(
					dummy_index, entry, 0);

				volume += page_dir_calc_reserved_space(1);

				ut_a(volume <= (4U << srv_page_size_shift)
				     / IBUF_PAGE_SIZE_PER_FREE_SPACE);
#endif
				ibuf_insert_to_index_page(
					entry, block, dummy_index, &mtr);
				break;

			case IBUF_OP_DELETE_MARK:
				ibuf_set_del_mark(
					entry, block, dummy_index, &mtr);
				break;

			case IBUF_OP_DELETE:
				ibuf_delete(entry, block, dummy_index, &mtr);
				/* Because ibuf_delete() will latch an
				insert buffer bitmap page, commit mtr
				before latching any further pages.
				Store and restore the cursor position. */
				ut_ad(rec == btr_pcur_get_rec(&pcur));
				ut_ad(page_rec_is_user_rec(rec));
				ut_ad(ibuf_rec_get_page_no(&mtr, rec)
				      == page_id.page_no());
				ut_ad(ibuf_rec_get_space(&mtr, rec)
				      == page_id.space());

				/* Mark the change buffer record processed,
				so that it will not be merged again in case
				the server crashes between the following
				mtr_commit() and the subsequent mtr_commit()
				of deleting the change buffer record. */
				btr_rec_set_deleted<true>(
					btr_pcur_get_block(&pcur),
					btr_pcur_get_rec(&pcur), &mtr);

				btr_pcur_store_position(&pcur, &mtr);
				btr_pcur_commit_specify_mtr(&pcur, &mtr);

				mtr.start();
				mtr.set_named_space(space);

				block->page.lock.x_lock_recursive();
				block->fix();
				mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);

				if (!ibuf_restore_pos(page_id, search_tuple,
						      BTR_MODIFY_LEAF,
						      &pcur, &mtr)) {

					ut_ad(mtr.has_committed());
					ibuf_dummy_index_free(dummy_index);
					goto loop;
				}

				break;
			default:
				ut_error;
			}

			ibuf_dummy_index_free(dummy_index);
		}

		/* Delete the record from ibuf */
		if (ibuf_delete_rec(page_id, &pcur, search_tuple, &mtr)) {
			/* Deletion was pessimistic and mtr was committed:
			we start from the beginning again */

			ut_ad(mtr.has_committed());
			goto loop;
		} else if (btr_pcur_is_after_last_on_page(&pcur)) {
			mtr.commit();
			goto loop;
		}
	}

reset_bit:
	if (space) {
		ibuf_reset_bitmap(page_id, zip_size, &mtr);
	}

	mtr.commit();
	ut_free(pcur.old_rec_buf);

	if (space) {
		space->release();
	}

	mem_heap_free(heap);

	return err;
}

/** Delete all change buffer entries for a tablespace. */
static void ibuf_delete_for_discarded_space(uint32_t space)
{
	btr_pcur_t	pcur;
	const rec_t*	ibuf_rec;
	mtr_t		mtr;

	dfield_t	dfield[IBUF_REC_FIELD_METADATA];
	dtuple_t	search_tuple {0,IBUF_REC_FIELD_METADATA,
				      IBUF_REC_FIELD_METADATA,dfield,0
				      ,nullptr
#ifdef UNIV_DEBUG
				      ,DATA_TUPLE_MAGIC_N
#endif /* UNIV_DEBUG */
	};
	byte space_id[4];
	mach_write_to_4(space_id, space);

	dfield_set_data(&dfield[0], space_id, 4);
	dfield_set_data(&dfield[1], field_ref_zero, 1);
	dfield_set_data(&dfield[2], field_ref_zero, 4);
	dtuple_set_types_binary(&search_tuple, IBUF_REC_FIELD_METADATA);
	/* Use page number 0 to build the search tuple so that we get the
	cursor positioned at the first entry for this space id */

	pcur.btr_cur.page_cur.index = ibuf_index;

loop:
	log_free_check();
	mtr.start();

	/* Position pcur in the insert buffer at the first entry for the
	space */
	if (btr_pcur_open_on_user_rec(&search_tuple, PAGE_CUR_GE,
				      BTR_MODIFY_LEAF, &pcur, &mtr)
	    != DB_SUCCESS) {
		goto leave_loop;
	}

	if (!btr_pcur_is_on_user_rec(&pcur)) {
		ut_ad(btr_pcur_is_after_last_on_page(&pcur));
		goto leave_loop;
	}

	for (;;) {
		ut_ad(btr_pcur_is_on_user_rec(&pcur));

		ibuf_rec = btr_pcur_get_rec(&pcur);

		/* Check if the entry is for this space */
		if (ibuf_rec_get_space(&mtr, ibuf_rec) != space) {

			goto leave_loop;
		}

		uint32_t page_no = ibuf_rec_get_page_no(&mtr, ibuf_rec);

		/* Delete the record from ibuf */
		if (ibuf_delete_rec(page_id_t(space, page_no),
				    &pcur, &search_tuple, &mtr)) {
			/* Deletion was pessimistic and mtr was committed:
			we start from the beginning again */

			ut_ad(mtr.has_committed());
clear:
			ut_free(pcur.old_rec_buf);
			goto loop;
		}

		if (btr_pcur_is_after_last_on_page(&pcur)) {
			mtr.commit();
			goto clear;
		}
	}

leave_loop:
	mtr.commit();
	ut_free(pcur.old_rec_buf);
}

/** Merge the change buffer to some pages. */
static void ibuf_read_merge_pages(const uint32_t* space_ids,
				  const uint32_t* page_nos, ulint n_stored)
{
	for (ulint i = 0; i < n_stored; i++) {
		const uint32_t space_id = space_ids[i];
		fil_space_t* s = fil_space_t::get(space_id);
		if (!s) {
tablespace_deleted:
			/* The tablespace was not found: remove all
			entries for it */
			ibuf_delete_for_discarded_space(space_id);
			while (i + 1 < n_stored
			       && space_ids[i + 1] == space_id) {
				i++;
			}
			continue;
		}

		const ulint zip_size = s->zip_size(), size = s->size;
		s->release();
		mtr_t mtr;

		if (UNIV_LIKELY(page_nos[i] < size)) {
			mtr.start();
			dberr_t err;
                        const page_id_t id{space_id, page_nos[i]};
			buf_block_t* block =
			buf_page_get_gen(id, zip_size, RW_X_LATCH, nullptr,
					 BUF_GET_POSSIBLY_FREED,
					 &mtr, &err);
			if (block) {
				err = ibuf_merge_or_delete_for_page(block, id,
                                                                    zip_size);
			}
			mtr.commit();
			if (err == DB_TABLESPACE_DELETED) {
				goto tablespace_deleted;
			}
		}

		ibuf_delete_recs(page_id_t(space_ids[i], page_nos[i]));
	}
}

/** Contract the change buffer by reading pages to the buffer pool. */
void ibuf_contract() // FIXME: use this
{
	mtr_t		mtr;
	btr_pcur_t	pcur;
	uint32_t	page_nos[IBUF_MAX_N_PAGES_MERGED];
	uint32_t	space_ids[IBUF_MAX_N_PAGES_MERGED];

	mtr.start();

	/* Open a cursor to a randomly chosen leaf of the tree, at a random
	position within the leaf */
	pcur.pos_state = BTR_PCUR_IS_POSITIONED;
	pcur.old_rec = nullptr;
	pcur.trx_if_known = nullptr;
	pcur.search_mode = PAGE_CUR_G;
	pcur.latch_mode = BTR_SEARCH_LEAF;

	btr_pcur_init(&pcur);

	if (!btr_cur_open_at_rnd_pos(ibuf_index, BTR_SEARCH_LEAF,
				     btr_pcur_get_btr_cur(&pcur), &mtr)) {
		return;
	}

	ut_ad(page_validate(btr_pcur_get_page(&pcur), ibuf_index));

	if (page_is_empty(btr_pcur_get_page(&pcur))) {
		/* If a B-tree page is empty, it must be the root page
		and the whole B-tree must be empty. InnoDB does not
		allow empty B-tree pages other than the root. */
		ut_ad(btr_pcur_get_block(&pcur)->page.id() == ibuf_root);
		mtr.commit();

		return;
	}

	ulint n_pages = 0;
	ibuf_get_merge_page_nos(btr_pcur_get_rec(&pcur), &mtr,
				space_ids, page_nos, &n_pages);
	mtr.commit();

	ibuf_read_merge_pages(space_ids, page_nos, n_pages);
}
