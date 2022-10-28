#ifndef JSON_SCHEMA_HELPER
#define JSON_SCHEMA_HELPER

/* Copyright (c) 2016, 2021, MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql_type_json.h"
#include <m_string.h>

/*
This structure is to get information only for "type", including
types for all keys(properties) in an object for any given key-value pair.
*/
typedef struct json_schema_type_info : public Sql_alloc
{
  enum json_value_types type;
  char* key_name;
  HASH properties;
  bool has_properties;
} st_json_schema_type_info;

int get_type_info_for_schema(st_json_schema_type_info *type_info,
                             json_engine_t *je, List<HASH> *hash_list, bool add_to_list);

bool json_key_equals(const char* key,  LEX_CSTRING val, int key_len);
bool json_assign_type(enum json_value_types *curr_type, json_engine_t *je);
bool handle_properties_keyword(st_json_schema_type_info *type_info,
                               json_engine_t *je,
                               List<HASH>*type_info_hash_list);

uchar* get_key_for_property(const uchar *buff, size_t *length,
                        my_bool /* unused */);

uchar* get_key_name_type_schema(const uchar *buff, size_t *length,
                        my_bool /* unused */);

uchar* get_key_name(const char *key_name, size_t *length,
                    my_bool /* unused */);
void json_get_normalized_string(json_engine_t *je, String *res,
                                int *error);

bool search_from_appropriate_hash(json_engine_t *je, uchar *norm_str,
                                  HASH *number_hash, HASH *string_hash,
                                  HASH *array_hash, HASH *object_hash);

bool json_insert_into_appropriate_hash(json_engine_t *je, uchar *norm_str,
                                       HASH *number_hash, HASH *string_hash,
                                       HASH *array_hash, HASH * object_hash);
#endif
