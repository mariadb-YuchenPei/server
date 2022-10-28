#ifndef JSON_SCHEMA_INCLUDED
#define JSON_SCHEMA_INCLUDED

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


/* This file defines all json schema classes. */

#include "sql_class.h"
#include "sql_type_json.h"
#include "json_schema_helper.h"

enum common_constraints_flag {HAS_NO_GEN_CONSTRAINT= 0, HAS_CONST= 2, HAS_ENUM= 4};
class Json_schema: public Sql_alloc
{
	public:
	  enum json_value_types type;
	  char *const_json_value;
	  HASH enum_values;
	  uint common_constraint_flag;
    char *key_name;
    Json_schema()
    {
      const_json_value= NULL;
      common_constraint_flag= HAS_NO_GEN_CONSTRAINT;
      type= JSON_VALUE_UNINITIALIZED;
      key_name= NULL;
    }
    virtual ~Json_schema() = default;

    virtual bool handle_common_keyword(const char* curr_key, int key_len,
                                      json_engine_t *je, List <HASH> *hash_list,
                                      st_json_schema_type_info *type_info);
    void set_keyword(char *name, int len, THD *thd);
    virtual bool handle_type_specific_keyword(const char* keyword, int key_len,
                                              json_engine_t *je, double *val,
                                              List <HASH> *hash_list,
                                              st_json_schema_type_info *type_info,
                                              List<HASH> *type_info_hash_list,
                                              List<Json_schema> *schema_list)
    { return false; }
    bool handle_keywords(json_engine_t *je, List <HASH> *hash_list,
                         st_json_schema_type_info *type_info,
                         List<HASH> *type_info_hash_list,
                         List<Json_schema> *schema_list);
    virtual bool validate_type_specific_constraint(json_engine_t *je)
    { return false;}
    virtual bool validate_json_for_common_constraint(json_engine_t *je);
    bool handle_annotations(json_engine_t *je, const char* curr_key, int key_len);
    virtual void cleanup() { return; }
};

enum bool_constraint_flag {HAS_NONE= 0, HAS_TRUE= 2, HAS_FALSE= 4};
class Json_schema_boolean : public Json_schema
{
  public:
    uint bool_constraint_flag_enum;
    uint bool_constraint_flag_const;
    Json_schema_boolean(enum json_value_types val_type)
    {
     type= val_type;
     bool_constraint_flag_enum= bool_constraint_flag_const= HAS_NONE;
    }
    bool handle_common_keyword(const char* curr_key,
                               int key_len, json_engine_t *je,
                               List <HASH> *hash_list,
                               st_json_schema_type_info *type_info);
    bool validate_json_for_common_constraint(json_engine_t *je);
};

enum null_constraint_flag {HAS_NO_NULL= 0, HAS_NULL= 2};
class Json_schema_null : public Json_schema
{
  public:
    uint null_constraint_flag_enum;
    uint null_constraint_flag_const;
    Json_schema_null()
    {
      null_constraint_flag_enum= null_constraint_flag_const= 0;
      type= JSON_VALUE_NULL;
    }
    bool handle_common_keyword(const char* curr_key, int key_len,
                               json_engine_t *je, List <HASH> *hash_list,
                               st_json_schema_type_info *type_info);
    bool validate_json_for_common_constraint(json_engine_t *je);
};

enum number_property_flag { HAS_NO_NUM_VALUE_CONSTRAINT=0, HAS_MIN=2,
                            HAS_EXCLUSIVE_MIN= 4, HAS_MAX=8,
                            HAS_EXCLUSIVE_MAX= 16, HAS_MULTIPLE_OF=32 };
class Json_schema_number : public Json_schema
{
	public:
    uint max, min, multiple_of, ex_min, ex_max;
    uint num_value_constraint;
    Json_schema_number()
    {
      type= JSON_VALUE_NUMBER;
      num_value_constraint= HAS_NO_NUM_VALUE_CONSTRAINT;
    }
    bool handle_type_specific_keyword(const char* keyword, int key_len,
                                      json_engine_t *je, double *val,
                                      List <HASH> *hash_list,
                                      st_json_schema_type_info *type_info,
                                      List<HASH> *type_info_hash_list,
                                      List<Json_schema> *schema_list);
    bool validate_type_specific_constraint(json_engine_t *je);
};

enum string_property_flag
{ HAS_NO_STR_VALUE_CONSTRAINT= 0, HAS_MAX_LEN= 2, HAS_MIN_LEN= 4, HAS_PATTERN= 8};

class Json_schema_string : public Json_schema
{
  public:
    double max_len, min_len;
    uint str_value_constraint;
    Regexp_processor_pcre re;
    Item *pattern;
    Json_schema_string()
    {
      type= JSON_VALUE_STRING;
      str_value_constraint= HAS_NO_STR_VALUE_CONSTRAINT;
    }
    bool handle_type_specific_keyword(const char* keyword, int key_len,
                                      json_engine_t *je, double *val,
                                      List <HASH> *hash_list,
                                      st_json_schema_type_info *type_info,
                                      List<HASH> *type_info_hash_list,
                                      List<Json_schema> *schema_list);
    bool validate_type_specific_constraint(json_engine_t *je);
    ~Json_schema_string()
    {
      if (str_value_constraint & HAS_PATTERN)
        re.cleanup();
    }
    void cleanup()
    {
      if (str_value_constraint & HAS_PATTERN)
        re.cleanup();
    }
};

 enum array_property_flag
 { HAS_NO_ARRAY_FLAG= 0, HAS_MAX_ITEMS= 2, HAS_MIN_ITEMS= 4,
  HAS_MAX_CONTAINS=8, HAS_MIN_CONTAINS= 16, HAS_UNIQUE= 32,
  HAS_PREFIX= 64, ALLOW_ADDITIONAL_ITEMS= 128 };

class Json_schema_array : public Json_schema
{
	public:
	  double max_items, min_items, min_contains, max_contains;
    enum json_value_types allowed_item_type;
    enum json_value_types contains_item_type;
    uint arr_value_constraint;
    List <Json_schema> prefix_items;
    Json_schema_array()
    {
      type= JSON_VALUE_ARRAY;
      arr_value_constraint= ALLOW_ADDITIONAL_ITEMS;
      allowed_item_type= contains_item_type= JSON_VALUE_UNINITIALIZED;
      prefix_items.empty();
    }
    bool handle_type_specific_keyword(const char* keyword, int key_len,
                                      json_engine_t *je, double *val,
                                      List <HASH> *hash_list,
                                      st_json_schema_type_info *type_info,
                                      List<HASH> *type_info_hash_list,
                                      List<Json_schema> *schema_list);
    bool validate_type_specific_constraint(json_engine_t *je);
};

typedef struct dependent_keyowrds
{
  String *property;
  List <String> dependents;
} st_dependent_keywords;

enum object_property_flag
{ HAS_NO_OBJECT_CONSTRAINT= 0, HAS_PROPERTY= 2, HAS_REQUIRED= 4,
  HAS_MAX_PROPERTIES= 8, HAS_MIN_PROPERTIES= 16,
  HAS_ADDITIONAL_PROPERTY_ALLOWED= 32, HAS_DEPENDENT_REQUIRED= 64 };
class Json_schema_object : public Json_schema
{
  public:
    int max_properties, min_properties;
    uint object_constraint;
    HASH properties;
    List<String> required_properties;
    List<st_dependent_keywords> dependent_required;

    Json_schema_object()
    {
      type= JSON_VALUE_OBJECT;
      object_constraint= HAS_NO_OBJECT_CONSTRAINT;
      required_properties.empty();
      dependent_required.empty();
    }
    bool handle_type_specific_keyword(const char* keyword, int key_len,
                                      json_engine_t *je, double *val,
                                      List <HASH> *hash_list,
                                      st_json_schema_type_info *type_info,
                                      List<HASH> *type_info_hash_list,
                                      List<Json_schema> *schema_list);
    bool validate_type_specific_constraint(json_engine_t *je);
};

Json_schema* create_object_and_handle_keywords(THD *thd,
                                               st_json_schema_type_info *type_info,
                                               json_engine_t *je, Json_schema *schema,
                                               List <HASH> *hash_list,
                                               List<HASH> *type_info_hash_list,
                                               List<Json_schema> *schema_list);

#endif
