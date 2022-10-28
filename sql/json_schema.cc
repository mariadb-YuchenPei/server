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
#include "json_schema.h"
#include "json_schema_helper.h"

Json_schema* create_object_and_handle_keywords(THD *thd,
                                               st_json_schema_type_info *type_info,
                                               json_engine_t *je, Json_schema *schema,
                                               List <HASH> *hash_list,
                                               List<HASH> *type_info_hash_list,
                                               List <Json_schema> *schema_list)
{

  if (type_info->type == JSON_VALUE_NUMBER)
    schema= new(thd->mem_root)Json_schema_number();
  else if (type_info->type == JSON_VALUE_STRING)
    schema= new(thd->mem_root)Json_schema_string();
  else if (type_info->type == JSON_VALUE_ARRAY)
    schema= new(thd->mem_root)Json_schema_array();
  else if (type_info->type == JSON_VALUE_OBJECT)
    schema= new(thd->mem_root)Json_schema_object();
  else if (type_info->type == JSON_VALUE_TRUE)
    schema= new (thd->mem_root)Json_schema_boolean(JSON_VALUE_TRUE);
  else if (type_info->type == JSON_VALUE_FALSE)
    schema= new (thd->mem_root)Json_schema_boolean(JSON_VALUE_FALSE);
  else if (type_info->type == JSON_VALUE_NULL)
    schema= new(thd->mem_root)Json_schema_null();

  if (type_info->key_name)
    schema->set_keyword(type_info->key_name, (int)(strlen(type_info->key_name)), thd);

  schema_list->push_back(schema);
  if (!schema->handle_keywords(je, hash_list, type_info, type_info_hash_list, schema_list))
    return schema;

  return NULL;
}

void Json_schema::set_keyword(char *name, int len, THD *thd)
{
  key_name= (char *) alloc_root(thd->mem_root, len+1);
  strncpy(key_name, (const char*)name, len);
  key_name[len]='\0';

  return;
}

bool Json_schema::handle_keywords(json_engine_t *je, List <HASH> *hash_list,
                                  st_json_schema_type_info *type_info,
                                  List<HASH>*type_info_hash_list,
                                  List<Json_schema> *schema_list)
{
  DBUG_EXECUTE_IF("json_check_min_stack_requirement",
                  {
                    long arbitrary_var;
                    long stack_used_up= (available_stack_size(current_thd->thread_stack, &arbitrary_var));
                    ALLOCATE_MEM_ON_STACK(my_thread_stack_size-stack_used_up-STACK_MIN_SIZE);
                  });
  if (check_stack_overrun(current_thd, STACK_MIN_SIZE , NULL))
    return 1;

  double val= 0;
  char *end;

  int level= je->stack_p, err;
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

          if (je->value_type == JSON_VALUE_NUMBER)
            val= je->s.cs->strntod((char *) je->value, je->value_len, &end, &err);

          if (json_key_equals((const char*)key_start,
                              { STRING_WITH_LEN("type") }, (int)(key_end-key_start)))
          {
            if (!json_value_scalar(je))
            {
              if (json_skip_level(je))
                return true;
            }
            /* type is already handled. */
            continue;
          }
          if (handle_annotations(je, (const char*)key_start, (int)(key_end-key_start)) &&
              handle_common_keyword((const char*)key_start, (int)(key_end-key_start),
                                    je, hash_list, type_info) &&
              handle_type_specific_keyword((const char*)key_start,
                                            (int)(key_end-key_start), je,
                                            je->value_type == JSON_VALUE_NUMBER ?
                                                               &val : NULL,
                                            hash_list, type_info,
                                            type_info_hash_list,
                                            schema_list))
          {
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
  return false;
}

bool
Json_schema::handle_annotations(json_engine_t *je, const char* curr_key,
                                int key_len)
{
  bool is_invalid_value_type= false, res= false;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("title") }, key_len) ||
      json_key_equals(curr_key, { STRING_WITH_LEN("description") }, key_len) ||
      json_key_equals(curr_key, { STRING_WITH_LEN("$comment") }, key_len) ||
      json_key_equals(curr_key, { STRING_WITH_LEN("$schema") }, key_len))
  {
    if (je->value_type != JSON_VALUE_STRING)
      is_invalid_value_type= true;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("deprecated") }, key_len) ||
           json_key_equals(curr_key, { STRING_WITH_LEN("readOnly") }, key_len) ||
           json_key_equals(curr_key, { STRING_WITH_LEN("writeOnly") }, key_len))
  {
    if (je->value_type != JSON_VALUE_TRUE && je->value_type != JSON_VALUE_FALSE)
      is_invalid_value_type= true;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("example") }, key_len))
  {
    if (je->value_type != JSON_VALUE_ARRAY)
      is_invalid_value_type= true;
    if (json_skip_level(je))
      return true;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("default") }, key_len))
  {
    if (je->value_type != this->type)
      is_invalid_value_type= true;
  }
  else
    return true;

  if (is_invalid_value_type)
  {
    res= true;
    String keyword(0);
    keyword.append((const char*)curr_key, key_len);
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
  }
  return res;
}
bool
Json_schema::handle_common_keyword(const char* curr_key, int key_len,
                                   json_engine_t *je,
                                   List <HASH> *hash_list,
                                   st_json_schema_type_info *type_info)
{
  bool res= false;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("const") }, key_len))
  {
    this->common_constraint_flag|= HAS_CONST;

    const char *start= (char*)je->value;
    const char *end= (char*)je->value+je->value_len;

    json_engine_t temp_je;
    String a_res("", 0, je->s.cs);
    int err;

    if (!json_value_scalar(je))
    {
      if (json_skip_level(je))
       return true;
      end= (char*)je->s.c_str;
    }

    String val((char*)je->value, end-start, je->s.cs);

    json_scan_start(&temp_je, je->s.cs, (const uchar *) val.ptr(),
                  (const uchar *) val.end());
    if (je->value_type != JSON_VALUE_STRING)
    {
      if (json_read_value(&temp_je))
        return true;
      json_get_normalized_string(&temp_je, &a_res, &err);
      if (err)
        return true;
    }
    else
    {
      a_res.append(val.ptr(), val.length(), je->s.cs);
    }

    this->const_json_value= (char*)alloc_root(current_thd->mem_root,
                                              a_res.length()+1);
    if (!const_json_value)
      return true;

    const_json_value[a_res.length()]= '\0';
    strncpy(const_json_value, (const char*)a_res.ptr(), a_res.length());
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("enum") }, key_len))
  {
    common_constraint_flag|= HAS_ENUM;
    if (my_hash_init(PSI_INSTRUMENT_ME,
                 &this->enum_values,
                 je->s.cs, 1024, 0, 0, (my_hash_get_key) get_key_name,
                  NULL, 0))
      return true;
    hash_list->push_back(&this->enum_values);

    if (je->value_type == JSON_VALUE_ARRAY)
    {
      int curr_level= je->stack_p;
      while(json_scan_next(je) == 0 && curr_level <= je->stack_p)
      {
        if (json_read_value(je))
          return true;

        if (je->value_type == this->type)
        {
          char *norm_str;
          int err= 1;
          String a_res;

          json_get_normalized_string(je, &a_res, &err);
          if (err)
            return true;

          norm_str= (char*)alloc_root(current_thd->mem_root,
                                      a_res.length()+1);
          if (!norm_str)
            return true;

          norm_str[a_res.length()]= '\0';
          strncpy(norm_str, (const char*)a_res.ptr(), a_res.length());
          if (my_hash_insert(&this->enum_values, (uchar*)norm_str))
            return true;
        }
        else
        {
          if (!json_value_scalar(je))
          {
            if (json_skip_level(je))
              return true;
          }
        }
      }
    }
  }
  else
    return true;

  return res;
}

bool Json_schema::validate_json_for_common_constraint(json_engine_t *je)
{
  if (this->common_constraint_flag & HAS_ENUM)
  {
    String norm_str((char*)"",0, je->s.cs);

    String a_res;
    int err= 1;

    json_get_normalized_string(je, &a_res, &err);
    if (err)
      return true;

    norm_str.append((const char*)a_res.ptr(), a_res.length(), je->s.cs);

    if (my_hash_search(&this->enum_values, (const uchar*)(norm_str.ptr()),
                       strlen((const char*)(norm_str.ptr()))))
      return false;
    else
      return true;
  }
  if (this->common_constraint_flag & HAS_CONST)
  {

    const char *start= (char*)je->value;
    const char *end= (char*)je->value+je->value_len;
    json_engine_t temp_je= *je;
    json_engine_t temp_je_2;
    String a_res;
    int err;

    if (!json_value_scalar(&temp_je))
    {
      if (json_skip_level(&temp_je))
      {
        *je= temp_je;
        return true;
      }
      end= (char*)temp_je.s.c_str;
    }
    String val((char*)temp_je.value, end-start, temp_je.s.cs);

    json_scan_start(&temp_je_2, temp_je.s.cs, (const uchar *) val.ptr(),
                    (const uchar *) val.end());

    if (temp_je.value_type != JSON_VALUE_STRING)
    {
      if (json_read_value(&temp_je_2))
      {
        *je= temp_je;
        return true;
      }
      json_get_normalized_string(&temp_je_2, &a_res, &err);
      if (err)
        return true;
    }
    else
      a_res.append(val.ptr(), val.length(), je->s.cs);

    if (a_res.length() == strlen(const_json_value) &&
        !strncmp((const char*)const_json_value, a_res.ptr(), a_res.length()))
      return false;
    return true;
  }
  if (this->type != je->value_type)
      return true;

  return false;
}


bool
Json_schema_boolean::handle_common_keyword(const char* curr_key,
                                           int key_len,
                                           json_engine_t *je,
                                           List <HASH> *hash_list,
                                           st_json_schema_type_info *type_info)
{
  if (json_key_equals(curr_key, { STRING_WITH_LEN("type") }, key_len))
  {
    return false;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("enum") }, key_len))
  {
    common_constraint_flag|= HAS_ENUM;

    int curr_level= je->stack_p;
    while(json_scan_next(je) == 0 && curr_level <= je->stack_p)
    {
      if (json_read_value(je))
        return true;
      if (je->value_type == JSON_VALUE_TRUE)
        bool_constraint_flag_enum|= HAS_TRUE;
      else if (je->value_type == JSON_VALUE_FALSE)
        bool_constraint_flag_enum|= HAS_FALSE;
      else if (!json_value_scalar(je))
      {
        if (json_skip_level(je))
          return true;
      }
    }
  }
  else if ((strlen("const")==key_len) &&
            !strncmp((const char*)curr_key, "const", key_len))
  {
    common_constraint_flag|= HAS_CONST;

    if (je->value_type == JSON_VALUE_TRUE)
      bool_constraint_flag_const|= HAS_TRUE;
    else if (je->value_type == JSON_VALUE_FALSE)
      bool_constraint_flag_const|= HAS_FALSE;
    else if (!json_value_scalar(je))
    {
      if (json_skip_level(je))
        return true;
    }
  }
  else
    return true;
  return false;
}

bool Json_schema_boolean::validate_json_for_common_constraint(json_engine_t *je)
{
  if (je->value_type != type)
  {
    return true;
  }
  else if (common_constraint_flag & HAS_ENUM)
  {
    if (je->value_type == JSON_VALUE_TRUE)
    {
      if (bool_constraint_flag_enum & HAS_TRUE)
        return false;
    }
    else if (je->value_type == JSON_VALUE_FALSE)
    {
      if (bool_constraint_flag_enum & HAS_FALSE)
        return false;
    }
    return true;
  }
  else if (common_constraint_flag & HAS_CONST)
  {
    if (je->value_type == JSON_VALUE_TRUE)
    {
      if (bool_constraint_flag_const & HAS_TRUE)
        return false;
    }
    if (je->value_type == JSON_VALUE_FALSE)
    {
      if (bool_constraint_flag_const & HAS_FALSE)
        return false;
    }
    return true;
  }

  return false;
}

bool Json_schema_null::validate_json_for_common_constraint(json_engine_t *je)
{
  if (je->value_type != type)
  {
    return true;
  }
  else if (common_constraint_flag & HAS_ENUM)
  {
    if (null_constraint_flag_enum & HAS_NULL)
      return false;
    return true;
  }
  else if (common_constraint_flag & HAS_CONST)
  {
    if (null_constraint_flag_const & HAS_NULL)
      return false;
    return true;
  }
  return false;
}

bool
Json_schema_null::handle_common_keyword(const char* curr_key,
                                        int key_len, json_engine_t *je,
                                        List <HASH> *hash_list,
                                        st_json_schema_type_info *type_info)
{
  if (json_key_equals(curr_key, { STRING_WITH_LEN("type") }, key_len))
  {
    return false;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("enum") }, key_len))
  {
    common_constraint_flag|= HAS_ENUM;

    int curr_level= je->stack_p;
    while(json_scan_next(je) == 0 && curr_level <= je->stack_p)
    {
      if (json_read_value(je))
        return true;

      if (je->value_type == JSON_VALUE_NULL)
        null_constraint_flag_enum|= HAS_NULL;
      else
      {
        if (!json_value_scalar(je))
        {
          if (json_skip_level(je))
            return true;
        }
      }
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("const") }, key_len))
  {
    if (je->value_type == JSON_VALUE_NULL)
      null_constraint_flag_const|= HAS_NULL;
  }
  else
    return true;

  return false;
}

bool
Json_schema_array::handle_type_specific_keyword(const char* curr_key,
                                                int key_len, json_engine_t *je,
                                                double *val, List<HASH> *hash_list,
                                                st_json_schema_type_info *type_info,
                                                List<HASH> *type_info_hash_list,
                                                List<Json_schema> *schema_list)
{

  bool is_invalid_value_type= false, res= false;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("maxItems") },
                      key_len))
  {
    if (*val < 0)
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
    max_items= *val;
    arr_value_constraint|= HAS_MAX_ITEMS;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("minItems") },
                           key_len))
  {
    if (*val < 0)
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
    min_items= *val;
    arr_value_constraint|= HAS_MIN_ITEMS;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("maxContains") },
                           key_len))
  {
    if (*val < 0)
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
    max_contains= *val;
    arr_value_constraint|= HAS_MAX_CONTAINS;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("minContains") },
                           key_len))
  {
    if (*val < 0)
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
    min_contains= *val;
    arr_value_constraint|= HAS_MIN_CONTAINS;
  }
  else if(json_key_equals(curr_key, { STRING_WITH_LEN("items") },
                          key_len))
  {
    if (je->value_type == JSON_VALUE_OBJECT)
    {
      if (json_scan_next(je) || json_read_value(je) ||
            json_assign_type(&allowed_item_type, je))
          return true;
    }
    else if (je->value_type == JSON_VALUE_FALSE ||
             je->value_type == JSON_VALUE_TRUE)
    {
      if (je->value_type == JSON_VALUE_FALSE)
       arr_value_constraint&= ~ALLOW_ADDITIONAL_ITEMS;
    }
    else
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("contains") },
                           key_len))
  {
    int curr_level= je->stack_p;
    while(json_scan_next(je)==0 && je->stack_p > curr_level-1)
    {
      const uchar *key_end, *key_start= je->s.c_str;
      do
      {
        key_end= je->s.c_str;
      } while (json_read_keyname_chr(je) == 0);

      /*if the keyword inside "contains" is not type */
      if (strncmp((const char*)"type", (const char*)key_start,
                     (int)(key_end-key_start)))
          return true;

        if (json_read_value(je) ||
            json_assign_type(&contains_item_type, je))
          return true;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("uniqueItems") },
                           key_len))
  {
    if (je->value_type == JSON_VALUE_TRUE)
      arr_value_constraint|= HAS_UNIQUE;
    else if (je->value_type == JSON_VALUE_FALSE)
      arr_value_constraint&= ~HAS_UNIQUE;
    else
    {
      res= true;
      is_invalid_value_type= true;
      goto end;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("prefixItems") },
                           key_len))
  {
    /*
      Each individual prefix is basically just another schema.
      So validate each of them individually.
    */
   arr_value_constraint|= HAS_PREFIX;

    int level= je->stack_p;
    while(json_scan_next(je)==0 && je->stack_p >= level)
    {
      if (json_read_value(je))
        return true;

      Json_schema *curr_json_schema= NULL;
      st_json_schema_type_info curr_type_info;
      curr_type_info.has_properties= false;
      json_engine_t temp_je;
      char *begin, *end;
      int len;
      curr_type_info.key_name= NULL;
      begin= (char*)je->value;

      if (json_skip_level(je))
        return true;

      end= (char*)je->s.c_str;
      len= (int)(end-begin);

      json_engine_t temp_je_2;

      json_scan_start(&temp_je, je->s.cs, (const uchar *) begin,
                  (const uchar *)begin+len);
      temp_je_2= temp_je;
      if (get_type_info_for_schema(&curr_type_info, &temp_je, type_info_hash_list, false))
       return true;

      if (!(curr_json_schema=
            create_object_and_handle_keywords(current_thd, &curr_type_info,
                                              &temp_je_2, curr_json_schema,
                                              hash_list,
                                              type_info_hash_list,
                                              schema_list)))
        return true;
      this->prefix_items.push_back(curr_json_schema);
      if (curr_type_info.has_properties)
      {
        my_hash_free(&curr_type_info.properties);
      }
    }
  }
  else
    res= true;

  res= false;

  end:
  if (is_invalid_value_type)
  {
    String keyword(0);
    keyword.append((const char*)curr_key, key_len);
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
  }
  return res;
}

bool
Json_schema_string::handle_type_specific_keyword(const char* curr_key,
                                                 int key_len,
                                                 json_engine_t *je,
                                                 double *val,
                                                 List <HASH> *hash_list,
                                                 st_json_schema_type_info *type_info,
                                                 List<HASH> *type_info_hash_list,
                                                 List<Json_schema> *schema_list)
{
  bool res= false, is_invalid_value_type= false;
  const char *curr_value= (const char*)je->value;
  int len= je->value_len;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("maxLength") },
                      key_len))
  {
    if (*val < 0)
    {
      is_invalid_value_type= true;
      goto end;
    }
    max_len= *val;
    str_value_constraint|= HAS_MAX_LEN;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("minLength") },
                           key_len))
  {
    if (*val < 0)
    {
      is_invalid_value_type= true;
      goto end;
    }
    min_len= *val;
    str_value_constraint|= HAS_MIN_LEN;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("pattern") },
                           key_len))
  {
    str_value_constraint|= HAS_PATTERN;
    my_repertoire_t repertoire= my_charset_repertoire(je->s.cs);
    pattern= current_thd->make_string_literal((const char*)je->value,
                                               je->value_len, repertoire);
    re.init(je->s.cs, 0);
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("format") }, key_len))
  {
    /*
      According to json schema draft 2019, the format keyword functions as annotation
      and only optionally as an assertion. The value of format attribute must be a string.
      For now, we treat these as annotations.
    */
    if (je->value_type != JSON_VALUE_STRING)
    {
      is_invalid_value_type= true;
      goto end;
    }
    if (json_key_equals(curr_value, { STRING_WITH_LEN("date-time") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("date") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("time") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("duration") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("email") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("idn-email") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("hostname") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("idn-hostname") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("ipv4") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("ipv6") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("uri") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("uri-reference") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("iri") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("iri-reference") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("uuid") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("json-pointer") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("relative-json-pointer") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("regex") }, len))
    {
      return false;
    }
    else
    {
      is_invalid_value_type= true;
      goto end;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("contentEncoding") },
                           key_len))
  {
    if (je->value_type != JSON_VALUE_STRING)
    {
      is_invalid_value_type= true;
      goto end;
    }
    if(!(json_key_equals(curr_value, { STRING_WITH_LEN("Base16") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("Base32") }, len) ||
        json_key_equals(curr_value, { STRING_WITH_LEN("Base64") }, len)))
    {
      is_invalid_value_type= true;
      goto end;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN(" contentMediaType") },
                           key_len))
  {
    if (je->value_type != JSON_VALUE_STRING)
    {
      is_invalid_value_type= true;
      goto end;
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("contentSchema") },
                           key_len))
  {
    if (je->value_type != JSON_VALUE_OBJECT)
    {
      is_invalid_value_type= true;
      goto end;
    }
    if (json_skip_level(je))
      return true;
  }
  else
    goto end;

  end:
  if (is_invalid_value_type)
  {
    String keyword(0);
    keyword.append((const char*)curr_key, key_len);
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
    return true;
  }
  return res;
}

bool
Json_schema_number::handle_type_specific_keyword(const char* curr_key,
                                                 int key_len,
                                                 json_engine_t *je,
                                                 double *val,
                                                 List <HASH> *hash_list,
                                                 st_json_schema_type_info *type_info,
                                                 List<HASH> *type_info_hash_list,
                                                 List<Json_schema> *schema_list)
{
  bool res= false, is_invalid_value_type= false;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("maximum") },
                      key_len))
  {
    max= (int)(*val);
    num_value_constraint|= HAS_MAX;
    goto end;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("minimum") },
                           key_len))
  {
    min= (int)(*val);
    num_value_constraint|= HAS_MIN;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("multipleOf") },
                           key_len))
  {
    /*
     value of "multipleOf" should be >= 0 according to rules for json schema
     mentioned in json schema draft.
    */
   if (*val <= 0)
   {
    is_invalid_value_type= true;
    res= true;
    goto end;
   }
   multiple_of= (int)(*val);
   num_value_constraint|= HAS_MULTIPLE_OF;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("exclusiveMaximum") },
                           key_len))
  {
    ex_max= (int)(*val);
    num_value_constraint|= HAS_EXCLUSIVE_MAX;
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("exclusiveMinimum") },
                           key_len))
  {
    ex_min= (int)(*val);
    num_value_constraint|= HAS_EXCLUSIVE_MIN;
  }
  else
    res= true;

  end:
  if (is_invalid_value_type)
  {
    String keyword(0);
    keyword.append((const char*)curr_key, key_len);
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
    return true;
  }
  return res;
}

bool
Json_schema_object::handle_type_specific_keyword(const char* curr_key,
                                                 int key_len,
                                                 json_engine_t *je,
                                                 double *val,
                                                 List <HASH> *hash_list,
                                                 st_json_schema_type_info *type_info,
                                                 List<HASH> *type_info_hash_list,
                                                 List<Json_schema> *schema_list)
{
  bool res= true;
  hash_list->push_back(&properties);
  bool is_invalid_value_type= 0;

  if (json_key_equals(curr_key, { STRING_WITH_LEN("properties") }, key_len))
  {
    if (je->value_type != JSON_VALUE_OBJECT)
    {
      is_invalid_value_type= true;
      goto end;
    }
    if (my_hash_init(PSI_INSTRUMENT_ME,
                     &this->properties,
                     je->s.cs, 1024, 0, 0,
                     (my_hash_get_key) get_key_name_type_schema,
                     NULL, 0))
      return true;
    hash_list->push_back(&this->properties);

    object_constraint|= HAS_PROPERTY;

    int level= je->stack_p;
    while (json_scan_next(je)==0 && level <= je->stack_p)
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

          st_json_schema_type_info *curr_keyword=NULL;
          Json_schema *schema= NULL;
          if ((curr_keyword=
               (st_json_schema_type_info*)my_hash_search(&(type_info->properties),
                                       (const uchar*)key_start, (int)(key_end-key_start))))
          {
            schema= create_object_and_handle_keywords(current_thd, curr_keyword,
                                                      je, schema, hash_list,
                                                      type_info_hash_list,
                                                      schema_list);
            if (my_hash_insert(&this->properties, (const uchar*)schema))
              return true;
          }
        }
      }
    }
  }
  else if (json_key_equals(curr_key, { STRING_WITH_LEN("required") }, key_len))
  {
    if (je->value_type != JSON_VALUE_ARRAY)
    {
      is_invalid_value_type= true;
      goto end;
    }
    object_constraint|= HAS_REQUIRED;

    int level= je->stack_p;
    while(json_scan_next(je)==0 && level <= je->stack_p)
    {
      if (json_read_value(je))
        return true;
      else
      {
        String *str= new (current_thd->mem_root)String((char*)je->value,
                                                    je->value_len, je->s.cs);
       this->required_properties.push_back(str);
      }
    }
  }
   else if (json_key_equals(curr_key, { STRING_WITH_LEN("maxProperties") },
                            key_len))
   {
    if (*val < 0)
    {
      is_invalid_value_type= true;
      goto end;
    }
    max_properties= (int)(*val);
    object_constraint|= HAS_MAX_PROPERTIES;
   }
    else if (json_key_equals(curr_key, { STRING_WITH_LEN("minProperties") },
                             key_len))
    {
      if (*val < 0)
      {
        is_invalid_value_type= true;
        goto end;
      }
      min_properties= (int)(*val);
      object_constraint|= HAS_MIN_PROPERTIES;
    }
    else if(json_key_equals(curr_key, { STRING_WITH_LEN("additionalProperties") },
                            key_len))
    {
     if (je->value_type == JSON_VALUE_FALSE)
        object_constraint&= ~HAS_ADDITIONAL_PROPERTY_ALLOWED;
     else if (je->value_type == JSON_VALUE_TRUE)
        object_constraint|= HAS_ADDITIONAL_PROPERTY_ALLOWED;
     else
     {
      is_invalid_value_type= true;
      goto end;
     }
    }
    else if (json_key_equals(curr_key, { STRING_WITH_LEN("dependentRequired") },
                             key_len))
    {
      if (je->value_type == JSON_VALUE_OBJECT)
      {
        int level1= je->stack_p;
        while (json_scan_next(je)==0 && level1 <= je->stack_p)
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

             if (je->value_type == JSON_VALUE_ARRAY)
             {
                st_dependent_keywords *curr_dependent_keywords=
                    (st_dependent_keywords *) alloc_root(current_thd->mem_root,
                                                sizeof(st_dependent_keywords));

                curr_dependent_keywords->property=
                            new (current_thd->mem_root)String((char*)key_start,
                                                    (int)(key_end-key_start), je->s.cs);

                curr_dependent_keywords->dependents.empty();

                int level2= je->stack_p;
                while (json_scan_next(je)==0 && level2 <= je->stack_p)
                {
                  if (json_read_value(je) || je->value_type != JSON_VALUE_STRING)
                  {
                    is_invalid_value_type= true;
                    goto end;
                  }
                  else
                  {
                    String *str=
                         new (current_thd->mem_root)String((char*)je->value,
                                                  je->value_len, je->s.cs);
                    curr_dependent_keywords->dependents.push_back(str);
                  }
                }
                dependent_required.push_back(curr_dependent_keywords);
              }
              else
              {
                is_invalid_value_type= true;
                goto end;
              }
            }
          }
        }
        if (!dependent_required.is_empty())
        {
          object_constraint|= HAS_DEPENDENT_REQUIRED;
        }
      }
    }
    else
      goto end;

  res= false;

  end:
  if (is_invalid_value_type)
  {
    String keyword(0);
    keyword.append((const char*)curr_key, key_len);
    my_error(ER_JSON_INVALID_VALUE_FOR_KEYWORD, MYF(0), keyword.ptr());
    return true;
  }
  return res;
}


bool Json_schema_number::validate_type_specific_constraint(json_engine_t *je)
{
  if (je->value_type != this->type)
   return true;

  char *end;
  int err, res= 1;
  uint curr_num_value_constraint=
                            this->num_value_constraint;

    double val= je->s.cs->strntod((char *) je->value,
                                  je->value_len, &end, &err);
    double temp= val / this->multiple_of;
    double is_multiple_of= curr_num_value_constraint & HAS_MULTIPLE_OF ?
                (temp - (long long int)temp) == 0 : true;

    if ((curr_num_value_constraint & HAS_MAX ?
         val <= this->max : true) &&
        (curr_num_value_constraint & HAS_EXCLUSIVE_MAX ?
         val < this->ex_max : true) &&
        (curr_num_value_constraint & HAS_MIN ?
         val >= this->min : true) &&
        (curr_num_value_constraint & HAS_EXCLUSIVE_MIN ?
         val > this->ex_min : true) &&
        (curr_num_value_constraint & HAS_MULTIPLE_OF ? is_multiple_of : true))
    {
        res= 0;
    }

  return res;
}

bool Json_schema_string::validate_type_specific_constraint(json_engine_t *je)
{
  bool pattern_matches= false;
  if (je->value_type != this->type)
  {
    return true;
  }

  if (str_value_constraint & HAS_PATTERN)
  {
    my_repertoire_t repertoire= my_charset_repertoire(je->s.cs);
    Item *str= current_thd->make_string_literal((const char*)je->value,
                                                je->value_len, repertoire);
    if (re.recompile(pattern))
      return true;
    if (re.exec(str, 0, 0))
      return true;
    pattern_matches= re.match();
  }

  if ((str_value_constraint & HAS_MAX_LEN ?
        je->value_len <= max_len : true) &&
      (str_value_constraint & HAS_MIN_LEN ?
       je->value_len >= min_len : true) &&
      (str_value_constraint & HAS_PATTERN ? pattern_matches : true))
    return false;
  return true;
}

bool Json_schema_array::validate_type_specific_constraint(json_engine_t *je)
{
  int number_of_elements= 0, level, contains= 0, res= 1;
  List_iterator<Json_schema> it(prefix_items);
  Json_schema *curr_prefix_item=NULL;
  HASH hash_number, hash_string, hash_object, hash_array;

  List <char> norm_str_list;
  norm_str_list.empty();

  if (arr_value_constraint & HAS_UNIQUE)
  {
    if (my_hash_init(PSI_INSTRUMENT_ME, &hash_number, je->s.cs, 1024, 0, 0,
                     (my_hash_get_key) get_key_name, NULL, 0) ||
        my_hash_init(PSI_INSTRUMENT_ME, &hash_string, je->s.cs, 1024, 0, 0,
                     (my_hash_get_key) get_key_name, NULL, 0) ||
        my_hash_init(PSI_INSTRUMENT_ME, &hash_array, je->s.cs, 1024, 0, 0,
                     (my_hash_get_key) get_key_name, NULL, 0) ||
        my_hash_init(PSI_INSTRUMENT_ME, &hash_object, je->s.cs, 1024, 0, 0,
                     (my_hash_get_key) get_key_name, NULL, 0))
      return true;
  }

  level= je->stack_p;
  while(json_scan_next(je)==0 && level <= je->stack_p)
  {
    if (json_read_value(je))
      goto end;
    if ((allowed_item_type != JSON_VALUE_UNINITIALIZED) &&
         (allowed_item_type != je->value_type))
      return true;
    if ((contains_item_type != JSON_VALUE_UNINITIALIZED) &&
        (contains_item_type == je->value_type))
        contains++;

    if ((arr_value_constraint & HAS_PREFIX) ||
        (arr_value_constraint & HAS_UNIQUE))
    {
      json_engine_t temp_je= *je;
      if (arr_value_constraint & HAS_PREFIX)
      {
        if ((curr_prefix_item=it++))
        {
          if (curr_prefix_item->validate_json_for_common_constraint(je) ||
              curr_prefix_item->validate_type_specific_constraint(je))
            goto end;
        }
        else
        {
          /*
            We read the value in array but curr_prefix_item is null.
            This means we either ran out of prefix items list or we didn't have
            a it to begin with. Return error if additional items are not allowed.
          */
           if (!(arr_value_constraint & ALLOW_ADDITIONAL_ITEMS))
             goto end;
        }
      }
      if (arr_value_constraint & HAS_UNIQUE)
      {
       int has_none= 0, has_true= 2, has_false= 4, has_null= 8, err= 1;
       char *norm_str;
       String a_res;

       json_get_normalized_string(&temp_je, &a_res, &err);

       if (err)
       {
         goto end;
       }
       norm_str= (char*)malloc(a_res.length()+1);
       if (!norm_str)
       {
         goto end;
       }
       norm_str[a_res.length()]= '\0';
       strncpy(norm_str, (const char*)a_res.ptr(), a_res.length());
       norm_str_list.push_back(norm_str);

       if (temp_je.value_type == JSON_VALUE_TRUE)
       {
         if (has_none & has_true)
           goto end;
         has_none= has_none | has_true;
       }
       else if (temp_je.value_type == JSON_VALUE_FALSE)
       {
         if (has_none & has_false)
           goto end;
         has_none= has_none | has_false;
       }
       else if (temp_je.value_type == JSON_VALUE_NULL)
       {
          if (has_none & has_null)
           goto end;
          has_none= has_none | has_null;
       }
       else
       {
         if (search_from_appropriate_hash(&temp_je, (uchar*)norm_str,
                        &hash_number, &hash_string, &hash_array, &hash_object))
         {
           if (json_insert_into_appropriate_hash(&temp_je, (uchar*)norm_str,
                           &hash_number, &hash_string, &hash_array, &hash_object))
            goto end;
          }
          else
            goto end;
       }
       a_res.set("", 0, je->s.cs);
      }
    }
    else
    {
      if (!json_value_scalar(je))
        json_skip_level(je);
    }
    number_of_elements++;
  }

  if ((contains_item_type != JSON_VALUE_UNINITIALIZED ?
            ((arr_value_constraint & HAS_MAX_CONTAINS ?
              contains <= max_contains : true) &&
             (arr_value_constraint & HAS_MIN_CONTAINS ?
              contains >= min_contains : true)) : true) &&
      (arr_value_constraint & HAS_MAX_ITEMS ?
       number_of_elements <= max_items : true) &&
      (arr_value_constraint & HAS_MIN_ITEMS ?
       number_of_elements >= min_items : true))
    res= false;

  end:
  if (!norm_str_list.is_empty())
  {
    List_iterator<char> it(norm_str_list);
    char *curr_norm_str;
    while ((curr_norm_str= it++))
      free(curr_norm_str);
    norm_str_list.empty();
  }
  if (arr_value_constraint & HAS_UNIQUE)
  {
    my_hash_free(&hash_number);
    my_hash_free(&hash_string);
    my_hash_free(&hash_array);
    my_hash_free(&hash_object);
  }
  return res;
}

bool Json_schema_object::validate_type_specific_constraint(json_engine_t *je)
{
  int properties_count= 0, curr_level= je->stack_p;
  bool res= true;
  HASH keywords;
  char *str=NULL;
  List <char> malloc_mem_list;
  malloc_mem_list.empty();

  if (object_constraint & (HAS_REQUIRED | HAS_DEPENDENT_REQUIRED))
  {
    if(my_hash_init(PSI_INSTRUMENT_ME, &keywords,
               je->s.cs, 1024, 0, 0, (my_hash_get_key) get_key_name,
               NULL, 0))
      return true;
  }
  if (object_constraint & HAS_PROPERTY)
  {
    while (json_scan_next(je)== 0 && je->stack_p >= curr_level)
  {
    switch (je->state)
    {
      case JST_KEY:
      {
        const uchar *key_end, *key_start= je->s.c_str;
        do
        {
          key_end= je->s.c_str;
        } while (json_read_keyname_chr(je) == 0);

        properties_count++;

        if (object_constraint & (HAS_REQUIRED | HAS_DEPENDENT_REQUIRED))
        {
          str= (char*)malloc((size_t)(key_end-key_start)+1);
          strncpy(str, (const char*)key_start, (int)(key_end-key_start));
          str[(int)(key_end-key_start)]='\0';
          if (my_hash_insert(&keywords, (const uchar*)str))
            goto error;
          malloc_mem_list.push_back(str);
        }

        if (json_read_value(je))
          goto error;

        Json_schema *curr_keyword;
        char *curr_key= (char*)malloc((int)(key_end-key_start)+1);
        strncpy(curr_key, (const char*)key_start, (int)(key_end-key_start));
        curr_key[(int)(key_end-key_start)]='\0';
        malloc_mem_list.push_back(curr_key);
        if (!(curr_keyword=
              (Json_schema*)my_hash_search(&this->properties,
                                   (const uchar*)curr_key, strlen(curr_key))))
        {
         if (!(object_constraint & HAS_ADDITIONAL_PROPERTY_ALLOWED))
           goto error;
        }
        else
        {
           if (curr_keyword->type == JSON_VALUE_UNINITIALIZED ||
               curr_keyword->validate_json_for_common_constraint(je) ||
               curr_keyword->validate_type_specific_constraint(je))
             goto error;
        }
      }
    }
   }
  }
  else
  {
    if (!json_value_scalar(je))
    {
      if (json_skip_level(je))
        goto error;
    }
  }
  if ((object_constraint & HAS_MAX_PROPERTIES ?
       properties_count > max_properties : false) ||
      (object_constraint & HAS_MIN_PROPERTIES ?
       properties_count < min_properties : false))
    goto error;

  if (object_constraint & HAS_REQUIRED)
  {
    List_iterator<String> it(required_properties);
    String *curr_str;
    while ((curr_str= it++))
    {
      if (!my_hash_search(&keywords, (const uchar*)curr_str->ptr(),
                          curr_str->length()))
        goto error;
    }
  }
  if (object_constraint & HAS_DEPENDENT_REQUIRED)
  {
    List_iterator<st_dependent_keywords> it(dependent_required);
    st_dependent_keywords *curr_keyword;
    while ((curr_keyword= it++))
    {
      if (my_hash_search(&keywords, (const uchar*)curr_keyword->property->ptr(),
                          curr_keyword->property->length()))
      {
        List_iterator<String> it2(curr_keyword->dependents);
        String *curr_depended_keyword;
        while ((curr_depended_keyword= it2++))
        {
          if (!my_hash_search(&keywords, (const uchar*)curr_depended_keyword->ptr(),
                              curr_depended_keyword->length()))
          {
            goto error;
          }
        }
      }
    }
  }

  res= false;

  error:
  if (!malloc_mem_list.is_empty())
  {
    List_iterator<char> it(malloc_mem_list);
    char *curr_ptr;
    while ((curr_ptr= it++))
      free(curr_ptr);
    malloc_mem_list.empty();
  }
  if (object_constraint & (HAS_REQUIRED | HAS_DEPENDENT_REQUIRED))
    my_hash_free(&keywords);

  return res;
}
