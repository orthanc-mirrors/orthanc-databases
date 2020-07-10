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


#include "OptimizedRoutes.h"
#include "../../Resources/Orthanc/Plugins/OrthancPluginCppWrapper.h"

#include <string>
#include <map>
#include <list>
#include <json/writer.h>
#include <json/value.h>
#include <boost/algorithm/string.hpp> // for boost::algorithm::split & boost::starts_with

static OrthancDatabases::IndexBackend* backend_;
static OrthancPluginContext* context_;

extern "C"
{

  // handles url like /optimized-routes/studies/{id)/instances-metadata?types=1,10
  OrthancPluginErrorCode RestGetStudiesInstancesMetadata(OrthancPluginRestOutput* output,
                                                         const char* /* url */,
                                                         const OrthancPluginHttpRequest* request)
  {
    std::list<int32_t> metadataTypes;
    for (uint32_t i = 0; i < request->getCount; i++)
    {
      if (strcmp(request->getKeys[i], "types") == 0)
      {
        std::string getValue(request->getValues[i]);
        std::vector<std::string> typesString;
        boost::algorithm::split(typesString, getValue, boost::is_any_of(","));

        for (size_t t = 0; t < typesString.size(); t++)
        {
          int32_t metadataType = boost::lexical_cast<int32_t>(typesString[t]);
          metadataTypes.push_back(metadataType);
        }
      }
    }

    std::map<std::string, std::map<int32_t, std::string>> result;
    std::string orthancId = std::string(request->groups[0]);
    backend_->GetStudyInstancesMetadata(result,
                                        orthancId,
                                        metadataTypes);

    Json::Value response(Json::objectValue);
    for (std::map<std::string, std::map<int32_t, std::string>>::const_iterator itInstance = result.begin(); itInstance != result.end(); itInstance++)
    {
      Json::Value instanceMetadatas;
      for (std::map<int32_t, std::string>::const_iterator itMetadata = itInstance->second.begin(); itMetadata != itInstance->second.end(); itMetadata++)
      {
        std::string id = boost::lexical_cast<std::string>(itMetadata->first);
        instanceMetadatas[id] = itMetadata->second;
      }

      response[itInstance->first] = instanceMetadatas;
    }

    Json::StyledWriter writer;
    std::string outputStr = writer.write(response);
    OrthancPluginAnswerBuffer(context_, output, outputStr.c_str(), outputStr.size(), "application/json");

    return OrthancPluginErrorCode_Success;
  }

  // handles url like /optimized-routes/studies/{id)/instances-ids
  OrthancPluginErrorCode RestGetStudiesInstancesIds(OrthancPluginRestOutput* output,
                                                    const char* /*url*/,
                                                    const OrthancPluginHttpRequest* request)
  {
    std::list<std::string> result;
    std::string orthancId = std::string(request->groups[0]);
    backend_->GetStudyInstancesIds(result,
                                   orthancId);

    Json::Value response(Json::arrayValue);
    for (std::list<std::string>::const_iterator itInstance = result.begin(); itInstance != result.end(); itInstance++)
    {
      response.append(*itInstance);
    }

    Json::StyledWriter writer;
    std::string outputStr = writer.write(response);
    OrthancPluginAnswerBuffer(context_, output, outputStr.c_str(), outputStr.size(), "application/json");

    return OrthancPluginErrorCode_Success;
  }
}

namespace OrthancDatabases {

  void OptimizedRoutes::EnableOptimizedRoutes(IndexBackend *backend, OrthancPluginContext *context)
  {
    backend_ = backend;
    context_ = context;
    // Register optimized rest routes (temporary)
    OrthancPluginRegisterRestCallbackNoLock(context_, "/optimized-routes/studies/(.*)/instances-metadata", RestGetStudiesInstancesMetadata);
    OrthancPluginRegisterRestCallbackNoLock(context_, "/optimized-routes/studies/(.*)/instances-ids", RestGetStudiesInstancesIds);
  }
}
