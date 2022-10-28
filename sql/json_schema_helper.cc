/* Copyright (c) 2016, 2022, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */


#include "mariadb.h"
#include "sql_class.h"
#include "sql_parse.h" // For check_stack_overrun
#include <m_string.h>
#include "json_schema_helper.h"


bool json_key_equals(const char* key,  LEX_CSTRING val, int key_len)
{
  return !strncmp(key, val.str, key_len);
}

bool json_assign_type(enum json_value_types *curr_type, json_engine_t *je)
{
  const char* curr_value= (const char*)je->value;
  int len= je->value_len;

  if (json_key_equals(curr_value, { STRING_WITH_LEN("number") }, len))
      *curr_type= JSON_VALUE_NUMBER;
  else if(json_key_equals(curr_value, { STRING_WITH_LEN("string") }, len))
      *curr_type= JSON_VALUE_STRING;
  else if(json_key_equals(curr_value, { STRING_WITH_LEN("array") }, len))
      *curr_type= JSON_VALUE_ARRAY;
  else if(json_key_equals(curr_value, { STRING_WITH_LEN("object") }, len))
      *curr_type= JSON_VALUE_OBJECT;
  else if (json_key_equals(curr_value, { STRING_WITH_LEN("true") }, len))
      *curr_type= JSON_VALUE_TRUE;
  else if (json_key_equals(curr_value, { STRING_WITH_LEN("false") }, len))
      *curr_type= JSON_VALUE_FALSE;
  else if (json_key_equals(curr_value, { STRING_WITH_LEN("null") }, len))
      *curr_type= JSON_VALUE_NULL;
  else
  {
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), "type");
    return true;
  }
  return false;
}

bool handle_properties_keyword(st_json_schema_type_info *type_info,
                               json_engine_t *je,
                               List<HASH>*type_info_hash_list)
{
  int curr_level= je->stack_p;
  while(json_scan_next(je)==0 && curr_level <= je->stack_p)
  {
    switch(je->state)
    {
      case JST_KEY:
      {
        const uchar *key_end, *key_start= je->s.c_str;
        do
        {
          key_end= je->s.c_str;
        } while (json_read_keyname_chr(je) == 0);

        st_json_schema_type_info *curr_keyword=
             (st_json_schema_type_info*)alloc_root(current_thd->mem_root,
                                                   sizeof(st_json_schema_type_info));
        curr_keyword->key_name= (char*)alloc_root(current_thd->mem_root,
                                                  (size_t)(key_end-key_start)+1);

        curr_keyword->has_properties= false;
        strncpy(curr_keyword->key_name, (const char*)key_start, (int)(key_end-key_start));
        curr_keyword->key_name[(int)(key_end-key_start)]='\0';

        if (json_read_value(je))
          return true;

        if (get_type_info_for_schema(curr_keyword, je, type_info_hash_list, true))
          return true;
        if (my_hash_insert(&type_info->properties, (const uchar*)curr_keyword))
          return true;
      }
    }
  }
  return false;
}

uchar* get_key_for_property(const uchar *buff, size_t *length,
                            my_bool /* unused */)
{
  st_json_schema_type_info* type_info = (st_json_schema_type_info*) buff;

  *length= strlen(type_info->key_name);
  return (uchar*) type_info->key_name;
}

int get_type_info_for_schema(st_json_schema_type_info *type_info,
                             json_engine_t *je, List<HASH> *type_info_hash_list, bool add_to_list)
{
  DBUG_EXECUTE_IF("json_check_min_stack_requirement",
                  {
                    long arbitrary_var;
                    long stack_used_up= (available_stack_size(current_thd->thread_stack, &arbitrary_var));
                    ALLOCATE_MEM_ON_STACK(my_thread_stack_size-stack_used_up-STACK_MIN_SIZE);
                  });
  if (check_stack_overrun(current_thd, STACK_MIN_SIZE , NULL))
    return 1;

  int has_type= false;

  int level= je->stack_p;
  while (json_scan_next(je)== 0 && je->stack_p >= level)
  {
    switch(je->state)
    {
      case JST_KEY:
      {
        const uchar *key_end, *key_start= je->s.c_str;
        do
        {
          key_end= je->s.c_str;
        } while (json_read_keyname_chr(je) == 0);

        if (json_read_value(je))
          return true;

        if (json_key_equals((const char*)key_start, { STRING_WITH_LEN("type") },
                            (int)(key_end-key_start)))
        {
          if (je->value_type == JSON_VALUE_ARRAY)
          {
            if (json_read_value(je))
             return true;
            if (json_assign_type(&(type_info->type), je))
             return true;
          }
          else if (je->value_type == JSON_VALUE_STRING)
          {
            if (json_assign_type(&(type_info->type), je))
              return true;
          }
          else
          {
            /*Invalid type*/
            String keyword(0);
            keyword.append((const char*)key_start, (int)(key_end-key_start));
            my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
            return true;
          }
          has_type= true;
        }
        else if (json_key_equals((const char*)key_start,
                                 { STRING_WITH_LEN("properties") },
                                 (int)(key_end-key_start)))
        {
          if (my_hash_init(PSI_INSTRUMENT_ME,
                           &type_info->properties,
                           je->s.cs, 1024, 0, 0,
                           (my_hash_get_key) get_key_for_property,
                           NULL, HASH_UNIQUE))
            return true;
          if (add_to_list)
            type_info_hash_list->push_back(&type_info->properties);
          type_info->has_properties= true;
          if (handle_properties_keyword(type_info, je, type_info_hash_list))
            return true;
        }
        else
        {
          /*
            skip keys like "item":{"type":"number"}. Because it will change
            the value type because of the "type" keyword.
          */
          if (!json_value_scalar(je))
          {
            if (json_skip_level(je))
              return true;
          }
        }
        break;
      }
    }
  }
  return je->s.error ? true: (!has_type ? true : false);
}


uchar* get_key_name_type_schema(const uchar *buff, size_t *length,
                        my_bool /* unused */)
{
   Json_schema *schema = (Json_schema*) buff;

  *length= strlen(schema->key_name);
  return (uchar*) schema->key_name;
}

uchar* get_key_name(const char *key_name, size_t *length,
                    my_bool /* unused */)
{
  *length= strlen(key_name);
  return (uchar*) key_name;
}

void json_get_normalized_string(json_engine_t *je, String *res,
                                int *error)
{
  char *val_begin= (char*)je->value, *val_end;
  String val;
  DYNAMIC_STRING a_res;

  if (init_dynamic_string(&a_res, NULL, 0, 0))
    goto error;

  if (!json_value_scalar(je))
  {
    if (json_skip_level(je))
      goto error;
  }

  val_end= json_value_scalar(je) ? val_begin+je->value_len :
                                   (char *)je->s.c_str;
  val.set((const char*)val_begin, val_end-val_begin, je->s.cs);

  if (je->value_type == JSON_VALUE_NUMBER ||
      je->value_type == JSON_VALUE_ARRAY ||
      je->value_type == JSON_VALUE_OBJECT)
  {
    if (json_normalize(&a_res, (const char*)val.ptr(),
                       val_end-val_begin, je->s.cs))
      goto error;
  }
  else if(je->value_type == JSON_VALUE_STRING)
  {
    strncpy((char*)a_res.str, val.ptr(), je->value_len);
    a_res.length= je->value_len;
  }

  res->append(a_res.str, a_res.length, je->s.cs);
  *error= 0;

  error:
  dynstr_free(&a_res);

  return;
}


bool search_from_appropriate_hash(json_engine_t *je, uchar *norm_str,
                                  HASH *number_hash, HASH *string_hash,
                                  HASH *array_hash, HASH *object_hash)
{
  bool res= true;
  char *found;

	if (je->value_type == JSON_VALUE_ARRAY)
  {
    if (!(found=(char*)my_hash_search(
                     array_hash,
                     (const uchar*)norm_str, strlen((const char*)norm_str))))
      goto error;
  }
  else if (je->value_type == JSON_VALUE_OBJECT)
  {
    if (!(found=(char*)my_hash_search(
                     object_hash,
                     (const uchar*)norm_str, strlen((const char*)norm_str))))
      goto error;
  }
  else if (je->value_type == JSON_VALUE_NUMBER)
  {
    if (!(found= (char*)my_hash_search(
            number_hash,
            (const uchar*)norm_str, strlen((const char*)norm_str))))
      goto error;
  }
  else if (je->value_type == JSON_VALUE_STRING)
  {
    if (!(found= (char*)my_hash_search(
            string_hash,
            (const uchar*)norm_str, strlen((const char*)norm_str))))
      goto error;
  }

  res= false;

  error:
  return res;
}

bool json_insert_into_appropriate_hash(json_engine_t *je, uchar *norm_str,
                                       HASH *number_hash, HASH *string_hash,
                                       HASH *array_hash, HASH * object_hash)
{
  bool res= true;

	if (je->value_type == JSON_VALUE_NUMBER)
  {
    if (my_hash_insert(number_hash, (const uchar*)(norm_str)))
       goto end;
  }
  else if (je->value_type == JSON_VALUE_STRING)
  {
    if (my_hash_insert(string_hash, (const uchar*)(norm_str)))
      goto end;
  }
  else if (je->value_type == JSON_VALUE_ARRAY)
  {
    if (my_hash_insert(array_hash, (const uchar*)(norm_str)))
      goto end;
  }
  else if (je->value_type == JSON_VALUE_OBJECT)
  {
    if (my_hash_insert(object_hash, (const uchar*)(norm_str)))
      goto end;
  }
  res= false;

  end:
  return res;
}