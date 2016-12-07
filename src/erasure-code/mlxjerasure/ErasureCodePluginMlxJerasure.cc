// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Chinac <shuxinxin@chinac.com>
 *
 * Author: xinxin shu <shuxinxin@chinac.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "ceph_ver.h"
#include "common/debug.h"
#include "ErasureCodeMlxJerasure.h"
#include "ErasureCodePluginMlxJerasure.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginMlxJerasure: ";
}

int ErasureCodePluginMlxJerasure::factory(const std::string& directory,
                                          ErasureCodeProfile& profile,
                                          ErasureCodeInterfaceRef *erasure_code,
                                          ostream *ss) {

  std::string nic;

  dout(20) << __func__ << ": " << profile << dendl;

  if (profile.find("nic") != profile.end())
    nic = profile.find("nic")->second;
  else
    return -ENOENT;

  ErasureCodeMlxJerasure *interface = new ErasureCodeMlxJerasure(nic.c_str());
  int r = interface->init(profile, ss);
  if (r) {
    delete interface;
    return r;
  }
  *erasure_code = ErasureCodeInterfaceRef(interface);

  return 0;
}

const char* __erasure_code_version()
{
  return CEPH_GIT_NICE_VER;
}

int __erasure_code_init(char* plugin_name, char* directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginMlxJerasure());
}
