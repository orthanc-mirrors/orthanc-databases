/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2022 Osimis S.A., Belgium
 * Copyright (C) 2021-2022 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


#include "MySQLIndex.h"
#include "../../Framework/MySQL/MySQLDatabase.h"
#include "../../Framework/Plugins/PluginInitialization.h"

#include <HttpClient.h>
#include <Logging.h>
#include <Toolbox.h>


extern "C"
{
  ORTHANC_PLUGINS_API int32_t OrthancPluginInitialize(OrthancPluginContext* context)
  {
    if (!OrthancDatabases::InitializePlugin(context, "MySQL", true))
    {
      return -1;
    }

    Orthanc::Toolbox::InitializeOpenSsl();
    Orthanc::HttpClient::GlobalInitialize();

    OrthancPlugins::OrthancConfiguration configuration;

    if (!configuration.IsSection("MySQL"))
    {
      LOG(WARNING) << "No available configuration for the MySQL index plugin";
      return 0;
    }

    OrthancPlugins::OrthancConfiguration mysql;
    configuration.GetSection(mysql, "MySQL");

    bool enable;
    if (!mysql.LookupBooleanValue(enable, "EnableIndex") ||
        !enable)
    {
      LOG(WARNING) << "The MySQL index is currently disabled, set \"EnableIndex\" "
                   << "to \"true\" in the \"MySQL\" section of the configuration file of Orthanc";
      return 0;
    }

    try
    {
      const size_t countConnections = mysql.GetUnsignedIntegerValue("IndexConnectionsCount", 1);

      OrthancDatabases::MySQLParameters parameters(mysql, configuration);
      OrthancDatabases::IndexBackend::Register(
        new OrthancDatabases::MySQLIndex(context, parameters), countConnections,
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
    LOG(WARNING) << "MySQL index is finalizing";
    OrthancDatabases::IndexBackend::Finalize();

    OrthancDatabases::MySQLDatabase::GlobalFinalization();
    Orthanc::HttpClient::GlobalFinalize();
    Orthanc::Toolbox::FinalizeOpenSsl();
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetName()
  {
    return "mysql-index";
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetVersion()
  {
    return ORTHANC_PLUGIN_VERSION;
  }
}
