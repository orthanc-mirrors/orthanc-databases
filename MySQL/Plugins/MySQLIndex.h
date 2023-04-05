/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2021-2023 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


#pragma once

#include "../../Framework/Plugins/IndexBackend.h"
#include "../../Framework/MySQL/MySQLParameters.h"

namespace OrthancDatabases
{
  class MySQLIndex : public IndexBackend 
  {
  private:
    MySQLParameters        parameters_;
    bool                   clearAll_;

  public:
    MySQLIndex(OrthancPluginContext* context,
               const MySQLParameters& parameters);

    void SetClearAll(bool clear)
    {
      clearAll_ = clear;
    }

    virtual IDatabaseFactory* CreateDatabaseFactory() ORTHANC_OVERRIDE;

    virtual void ConfigureDatabase(DatabaseManager& database) ORTHANC_OVERRIDE;
 
    virtual bool HasRevisionsSupport() const ORTHANC_OVERRIDE
    {
      return false;  // TODO - REVISIONS
    }
    
    virtual int64_t CreateResource(DatabaseManager& manager,
                                   const char* publicId,
                                   OrthancPluginResourceType type)
      ORTHANC_OVERRIDE;

    virtual void DeleteResource(IDatabaseBackendOutput& output,
                                DatabaseManager& manager,
                                int64_t id) ORTHANC_OVERRIDE;

    virtual int64_t GetLastChangeIndex(DatabaseManager& manager) ORTHANC_OVERRIDE;

    virtual bool HasCreateInstance() const  ORTHANC_OVERRIDE
    {
      return true;
    }

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void CreateInstance(OrthancPluginCreateInstanceResult& result,
                                DatabaseManager& manager,
                                const char* hashPatient,
                                const char* hashStudy,
                                const char* hashSeries,
                                const char* hashInstance)
      ORTHANC_OVERRIDE;
#endif
  };
}
