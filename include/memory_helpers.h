/* Copyright (c) 2012, 2020, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA */

#pragma once

#include <utility>
#include "my_global.h"
#include "my_atomic.h"

template <typename Ty> class Ptr_base;
template <typename Ty> class Reference_counter;

/*
  Shared_ptr is a smart pointer that retains shared ownership of an object
  through a pointer. Several Shared_ptr objects may own the same object.
  The object is destroyed and its memory deallocated when either
  of the following happens:
    - the last remaining Shared_ptr owning the object is destroyed;
    - the last remaining Shared_ptr owning the object is assigned
      another pointer via operator= or reset().

  All member functions (including copy constructor and copy assignment) can be
  called by multiple threads on different instances of Shared_ptr without
  additional synchronization even if these instances are copies and share
  ownership of the same object.

  This implementation is inspired by the STL's std::shared_ptr
  and has similar (though less powerful) interface.
*/
template <typename Type> class Shared_ptr : public Ptr_base<Type>
{
public:
  Shared_ptr() noexcept= default;

  // Construct empty shared_ptr
  Shared_ptr(nullptr_t) noexcept {}

  // Construct Shared_ptr object that owns object obj
  explicit Shared_ptr(Type *obj)
  {
    this->managed_obj= obj;
    this->ref_counter= new Reference_counter<Type>(obj);
  }

  // Construct Shared_ptr object that owns same resource as other
  Shared_ptr(const Shared_ptr &other) noexcept
  {
    this->copy_construct_from(other);
  }

  // Construct Shared_ptr object that takes resource from right
  Shared_ptr(Shared_ptr &&right) noexcept
  {
    this->move_construct_from(std::move(right));
  }

  ~Shared_ptr() noexcept { this->dec_ref_count(); }

  Shared_ptr &operator=(const Shared_ptr &right) noexcept
  {
    Shared_ptr(right).swap(*this);
    return *this;
  }

  template <class Type2>
  Shared_ptr &operator=(const Shared_ptr<Type2> &right) noexcept
  {
    Shared_ptr(right).swap(*this);
    return *this;
  }

  Shared_ptr &operator=(Shared_ptr &&right) noexcept
  {
    Shared_ptr(std::move(right)).swap(*this);
    return *this;
  }

  template <class Type2> Shared_ptr &operator=(Shared_ptr<Type2> &&right) noexcept
  {
    Shared_ptr(std::move(right)).swap(*this);
    return *this;
  }

  void swap(Shared_ptr &other) noexcept { Ptr_base<Type>::swap(other); }

  // Release resource and convert to empty shared_ptr object
  void reset() noexcept { Shared_ptr().swap(*this); }

  // Release, take ownership of obj
  template <typename Type2> void reset(Type2 *obj) noexcept
  {
    Shared_ptr(obj).swap(*this);
  }

  using Ptr_base<Type>::get;

  template <typename Type2= Type> Type2 &operator*() const noexcept
  {
    return *get();
  }

  template <typename Type2= Type> Type2 *operator->() const noexcept
  {
    return get();
  }

  explicit operator bool() const noexcept { return get() != nullptr; }
};


/* Base class for reference counting */
class Reference_counter_base
{
protected:
  Reference_counter_base() noexcept= default;

public:
  /* Forbid copying and moving */
  Reference_counter_base(const Reference_counter_base &)= delete;
  Reference_counter_base(Reference_counter_base &&)= delete;
  Reference_counter_base &operator=(const Reference_counter_base &)= delete;
  Reference_counter_base &operator=(Reference_counter_base &&)= delete;

  virtual ~Reference_counter_base() noexcept { }

  // Increment use count
  void inc_ref_count() noexcept
  {
    my_atomic_add32(&uses, 1);
  }

  // Decrement use count
  void dec_ref_count() noexcept
  {
    int32 prev_value= 0;
    if ((prev_value= my_atomic_add32(&uses, -1)) == 1)
    {
      destroy();
    }
  }

  unsigned long use_count() noexcept { return my_atomic_load32(&uses); }

private:
  virtual void destroy() noexcept= 0; /* destroy managed resource */

  volatile int32 uses= 1;
};


template <typename Type>
class Reference_counter : public Reference_counter_base
{ 
public:
  explicit Reference_counter(Type *obj) :
    Reference_counter_base(), managed_obj(obj) 
  {}

private:
  virtual void destroy() noexcept override
  { 
    /* destroy managed resource */
    delete managed_obj;
  }

  Type *managed_obj;
};


// Base class for Shared_ptr and Weak_ptr
template <typename Type> class Ptr_base
{
public:
  long use_count() const noexcept
  {
    return ref_counter ? ref_counter->use_count() : 0;
  }

  /* Forbid copying and moving */
  Ptr_base(const Ptr_base &)= delete;
  Ptr_base(Ptr_base &&)= delete;
  Ptr_base &operator=(const Ptr_base &)= delete;
  Ptr_base &operator=(Ptr_base &&)= delete;

protected:
  Type *get() const noexcept { return managed_obj; }

  Ptr_base() noexcept= default;

  // Shared_ptr's (converting) move ctor
  template <typename Type2>
  void move_construct_from(Ptr_base<Type2> &&other) noexcept
  {
    managed_obj= other.managed_obj;
    ref_counter= other.ref_counter;

    other.managed_obj= nullptr;
    other.ref_counter= nullptr;
  }

  // Shared_ptr's (converting) copy ctor
  template <typename Type2>
  void copy_construct_from(const Shared_ptr<Type2> &other) noexcept
  {
    other.inc_ref_count();

    managed_obj= other.managed_obj;
    ref_counter= other.ref_counter;
  }

  void inc_ref_count() const noexcept
  {
    if (ref_counter)
    {
      ref_counter->inc_ref_count();
    }
  }

  void dec_ref_count() noexcept
  { 
    if (ref_counter)
    {
      ref_counter->dec_ref_count();
    }
  }

  void swap(Ptr_base &right) noexcept
  { 
    auto tmpObj= this->managed_obj;
    this->managed_obj= right.managed_obj;
    right.managed_obj= tmpObj;

    auto tmpRefCnt= this->ref_counter;
    this->ref_counter= right.ref_counter;
    right.ref_counter= tmpRefCnt;
  }
  
private:
  Type *managed_obj= nullptr;
  Reference_counter_base *ref_counter= nullptr;

  template <class Type0> friend class Ptr_base;

  friend Shared_ptr<Type>;
};

