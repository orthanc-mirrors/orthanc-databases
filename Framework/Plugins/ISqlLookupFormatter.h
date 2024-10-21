/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2024 Orthanc Team SRL, Belgium
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


/**
 * NB: Until 2024-09-09, this file was synchronized with the following
 * folder from the Orthanc main project:
 * https://orthanc.uclouvain.be/hg/orthanc/file/default/OrthancServer/Sources/Search/
 **/


#pragma once

#include "MessagesToolbox.h"

#include <boost/noncopyable.hpp>
#include <vector>

namespace OrthancDatabases
{
  class DatabaseConstraints;
  class FindRequest;

  enum LabelsConstraint
  {
    LabelsConstraint_All,
    LabelsConstraint_Any,
    LabelsConstraint_None
  };

  class ISqlLookupFormatter : public boost::noncopyable
  {
  public:
    virtual ~ISqlLookupFormatter()
    {
    }

    virtual std::string GenerateParameter(const std::string& value) = 0;

    virtual std::string FormatResourceType(Orthanc::ResourceType level) = 0;

    virtual std::string FormatWildcardEscape() = 0;

    virtual std::string FormatLimits(uint64_t since, uint64_t count) = 0;

    virtual std::string FormatNull(const char* type) = 0;

    /**
     * Whether to escape '[' and ']', which is only needed for
     * MSSQL. New in Orthanc 1.10.0, from the following changeset:
     * https://orthanc.uclouvain.be/hg/orthanc-databases/rev/389c037387ea
     **/
    virtual bool IsEscapeBrackets() const = 0;

    static void GetLookupLevels(Orthanc::ResourceType& lowerLevel,
                                Orthanc::ResourceType& upperLevel,
                                const Orthanc::ResourceType& queryLevel,
                                const DatabaseConstraints& lookup);

    static void Apply(std::string& sql,
                      ISqlLookupFormatter& formatter,
                      const DatabaseConstraints& lookup,
                      Orthanc::ResourceType queryLevel,
                      const std::set<std::string>& labels,  // New in Orthanc 1.12.0
                      LabelsConstraint labelsConstraint,    // New in Orthanc 1.12.0
                      size_t limit);

    static void ApplySingleLevel(std::string& sql,
                                 ISqlLookupFormatter& formatter,
                                 const DatabaseConstraints& lookup,
                                 Orthanc::ResourceType queryLevel,
                                 const std::set<std::string>& labels,  // New in Orthanc 1.12.0
                                 LabelsConstraint labelsConstraint,    // New in Orthanc 1.12.0
                                 size_t limit);

#if ORTHANC_PLUGINS_HAS_INTEGRATED_FIND == 1
    static void Apply(std::string& sql,
                      ISqlLookupFormatter& formatter,
                      const Orthanc::DatabasePluginMessages::Find_Request& request);
#endif
  };
}
