/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2024 Osimis S.A., Belgium
 * Copyright (C) 2021-2024 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


#include "PostgreSQLIndex.h"
#include "../../Framework/Plugins/PluginInitialization.h"

#include <Logging.h>
#include <Toolbox.h>

#include <google/protobuf/any.h>

#define ORTHANC_PLUGIN_NAME "postgresql-index"


extern "C"
{
  ORTHANC_PLUGINS_API int32_t OrthancPluginInitialize(OrthancPluginContext* context)
  {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (!OrthancDatabases::InitializePlugin(context, ORTHANC_PLUGIN_NAME, "PostgreSQL", true))
    {
      return -1;
    }

    Orthanc::Toolbox::InitializeOpenSsl();

    OrthancPlugins::OrthancConfiguration configuration;

    if (!configuration.IsSection("PostgreSQL"))
    {
      LOG(WARNING) << "No available configuration for the PostgreSQL index plugin";
      return 0;
    }

    OrthancPlugins::OrthancConfiguration postgresql;
    configuration.GetSection(postgresql, "PostgreSQL");

    bool enable;
    if (!postgresql.LookupBooleanValue(enable, "EnableIndex") ||
        !enable)
    {
      LOG(WARNING) << "The PostgreSQL index is currently disabled, set \"EnableIndex\" "
                   << "to \"true\" in the \"PostgreSQL\" section of the configuration file of Orthanc";
      return 0;
    }

    try
    {
      const size_t countConnections = postgresql.GetUnsignedIntegerValue("IndexConnectionsCount", 1);

      OrthancDatabases::PostgreSQLParameters parameters(postgresql);
      OrthancDatabases::IndexBackend::Register(
        new OrthancDatabases::PostgreSQLIndex(context, parameters), countConnections,
        parameters.GetMaxConnectionRetries());
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
    LOG(WARNING) << "PostgreSQL index is finalizing";
    OrthancDatabases::IndexBackend::Finalize();
    Orthanc::Toolbox::FinalizeOpenSsl();
    google::protobuf::ShutdownProtobufLibrary();
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetName()
  {
    return ORTHANC_PLUGIN_NAME;
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetVersion()
  {
    return ORTHANC_PLUGIN_VERSION;
  }
}
