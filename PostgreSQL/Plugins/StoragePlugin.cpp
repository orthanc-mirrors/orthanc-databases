/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2020 Osimis S.A., Belgium
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


#include "PostgreSQLStorageArea.h"
#include "../../Framework/Plugins/PluginInitialization.h"

#include <Core/Logging.h>

extern "C"
{
  ORTHANC_PLUGINS_API int32_t OrthancPluginInitialize(OrthancPluginContext* context)
  {
    if (!OrthancDatabases::InitializePlugin(context, "PostgreSQL", false))
    {
      return -1;
    }

    OrthancPlugins::OrthancConfiguration configuration;

    if (!configuration.IsSection("PostgreSQL"))
    {
      LOG(WARNING) << "No available configuration for the PostgreSQL storage area plugin";
      return 0;
    }

    OrthancPlugins::OrthancConfiguration postgresql;
    configuration.GetSection(postgresql, "PostgreSQL");

    bool enable;
    if (!postgresql.LookupBooleanValue(enable, "EnableStorage") ||
        !enable)
    {
      LOG(WARNING) << "The PostgreSQL storage area is currently disabled, set \"EnableStorage\" "
                   << "to \"true\" in the \"PostgreSQL\" section of the configuration file of Orthanc";
      return 0;
    }

    try
    {
      OrthancDatabases::PostgreSQLParameters parameters(postgresql);
      OrthancDatabases::StorageBackend::Register
        (context, new OrthancDatabases::PostgreSQLStorageArea(parameters));
    }
    catch (Orthanc::OrthancException& e)
    {
      LOG(ERROR) << e.What();
      return -1;
    }
    catch (...)
    {
      LOG(ERROR) << "Native exception while initializing the plugin";
      return -1;
    }

    return 0;
  }


  ORTHANC_PLUGINS_API void OrthancPluginFinalize()
  {
    LOG(WARNING) << "PostgreSQL storage area is finalizing";
    OrthancDatabases::StorageBackend::Finalize();
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetName()
  {
    return "postgresql-storage";
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetVersion()
  {
    return ORTHANC_PLUGIN_VERSION;
  }
}
