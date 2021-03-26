/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2021 Osimis S.A., Belgium
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 **/



/**
 * NOTE: Until Orthanc 1.4.0, this file was part of the Orthanc source
 * distribution. This file is now part of "orthanc-databases", in
 * order to uncouple its evolution from the Orthanc core.
 **/

#pragma once

#include "IDatabaseBackend.h"


namespace OrthancDatabases
{  
  /**
   * @brief Bridge between C and C++ database engines.
   * 
   * Class creating the bridge between the C low-level primitives for
   * custom database engines, and the high-level IDatabaseBackend C++
   * interface, for Orthanc <= 1.9.1.
   *
   * @ingroup Callbacks
   **/
  class DatabaseBackendAdapterV2
  {
  private:
    // This class cannot be instantiated
    DatabaseBackendAdapterV2()
    {
    }

  public:
    class Output;
    
    class Factory : public IDatabaseBackendOutput::IFactory
    {
    private:
      OrthancPluginContext*         context_;
      OrthancPluginDatabaseContext* database_;

    public:
      Factory(OrthancPluginContext*         context,
              OrthancPluginDatabaseContext* database) :
        context_(context),
        database_(database)
      {
      }

      virtual IDatabaseBackendOutput* CreateOutput() ORTHANC_OVERRIDE;
    };


    /**
     * Register a custom database back-end written in C++.
     *
     * @param context The Orthanc plugin context, as received by OrthancPluginInitialize().
     * @param backend Your custom database engine.
     **/

    static void Register(IDatabaseBackend& backend);
  };
}