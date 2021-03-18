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


#include "PostgreSQLIndex.h"

#include "../../Framework/Plugins/GlobalProperties.h"
#include "../../Framework/PostgreSQL/PostgreSQLDatabase.h"
#include "../../Framework/PostgreSQL/PostgreSQLTransaction.h"
#include "PostgreSQLDefinitions.h"

#include <EmbeddedResources.h>  // Auto-generated file

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>


namespace Orthanc
{
  // Some aliases for internal properties
  static const GlobalProperty GlobalProperty_HasTrigramIndex = GlobalProperty_DatabaseInternal0;
  static const GlobalProperty GlobalProperty_HasCreateInstance = GlobalProperty_DatabaseInternal1;
  static const GlobalProperty GlobalProperty_HasFastCountResources = GlobalProperty_DatabaseInternal2;
  static const GlobalProperty GlobalProperty_GetLastChangeIndex = GlobalProperty_DatabaseInternal3;
}


namespace OrthancDatabases
{
  IDatabase* PostgreSQLIndex::OpenInternal()
  {
    uint32_t expectedVersion = 6;

    if (GetContext())   // "GetContext()" can possibly be NULL in the unit tests
    {
      expectedVersion = OrthancPluginGetExpectedDatabaseVersion(GetContext());
    }

    // Check the expected version of the database
    if (expectedVersion != 6)
    {
      LOG(ERROR) << "This database plugin is incompatible with your version of Orthanc "
                 << "expecting the DB schema version " << expectedVersion 
                 << ", but this plugin is only compatible with version 6";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Plugin);
    }

    std::unique_ptr<PostgreSQLDatabase> db(new PostgreSQLDatabase(parameters_));

    db->Open();

    if (parameters_.HasLock())
    {
      db->AdvisoryLock(POSTGRESQL_LOCK_INDEX);
    }

    {
      PostgreSQLDatabase::TransientAdvisoryLock lock(*db, POSTGRESQL_LOCK_DATABASE_SETUP);

      if (clearAll_)
      {
        db->ClearAll();
      }

      {
        PostgreSQLTransaction t(*db);

        if (!db->DoesTableExist("Resources"))
        {
          std::string query;

          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_PREPARE_INDEX);
          db->Execute(query);

          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabaseSchemaVersion, expectedVersion);
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, 1);
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_HasTrigramIndex, 0);
        }
          
        if (!db->DoesTableExist("Resources"))
        {
          LOG(ERROR) << "Corrupted PostgreSQL database";
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);        
        }

        int version = 0;
        if (!LookupGlobalIntegerProperty(version, *db, t, Orthanc::GlobalProperty_DatabaseSchemaVersion) ||
            version != 6)
        {
          LOG(ERROR) << "PostgreSQL plugin is incompatible with database schema version: " << version;
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }

        int revision;
        if (!LookupGlobalIntegerProperty(revision, *db, t, Orthanc::GlobalProperty_DatabasePatchLevel))
        {
          revision = 1;
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);
        }

        if (revision != 1)
        {
          LOG(ERROR) << "PostgreSQL plugin is incompatible with database schema revision: " << revision;
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }

        t.Commit();
      }

      {
        PostgreSQLTransaction t(*db);

        int hasTrigram = 0;
        if (!LookupGlobalIntegerProperty(hasTrigram, *db, t,
                                         Orthanc::GlobalProperty_HasTrigramIndex) ||
            hasTrigram != 1)
        {
          /**
           * Apply fix for performance issue (speed up wildcard search
           * by using GIN trigrams). This implements the patch suggested
           * in issue #47, BUT we also keep the original
           * "DicomIdentifiersIndexValues", as it leads to better
           * performance for "strict" searches (i.e. searches involving
           * no wildcard).
           * https://www.postgresql.org/docs/current/static/pgtrgm.html
           * https://bitbucket.org/sjodogne/orthanc/issues/47/index-improvements-for-pg-plugin
           **/
          try
          {
            // We've observed 9 minutes on DB with 100000 studies
            LOG(WARNING) << "Trying to enable trigram matching on the PostgreSQL database "
                         << "to speed up wildcard searches. This may take several minutes";

            db->Execute(
              "CREATE EXTENSION IF NOT EXISTS pg_trgm; "
              "CREATE INDEX DicomIdentifiersIndexValues2 ON DicomIdentifiers USING gin(value gin_trgm_ops);");

            SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_HasTrigramIndex, 1);
            LOG(WARNING) << "Trigram index has been created";

            t.Commit();
          }
          catch (Orthanc::OrthancException&)
          {
            LOG(WARNING) << "Performance warning: Your PostgreSQL server does "
                         << "not support trigram matching";
            LOG(WARNING) << "-> Consider installing the \"pg_trgm\" extension on the "
                         << "PostgreSQL server, e.g. on Debian: sudo apt install postgresql-contrib";
          }
        }
        else
        {
          t.Commit();
        }
      }

      {
        PostgreSQLTransaction t(*db);

        int property = 0;
        if (!LookupGlobalIntegerProperty(property, *db, t,
                                         Orthanc::GlobalProperty_HasCreateInstance) ||
            property != 2)
        {
          LOG(INFO) << "Installing the CreateInstance extension";

          if (property == 1)
          {
            // Drop older, experimental versions of this extension
            db->Execute("DROP FUNCTION CreateInstance("
                        "IN patient TEXT, IN study TEXT, IN series TEXT, in instance TEXT)");
          }
        
          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_CREATE_INSTANCE);
          db->Execute(query);

          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_HasCreateInstance, 2);
        }

      
        if (!LookupGlobalIntegerProperty(property, *db, t,
                                         Orthanc::GlobalProperty_GetTotalSizeIsFast) ||
            property != 1)
        {
          LOG(INFO) << "Installing the FastTotalSize extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_FAST_TOTAL_SIZE);
          db->Execute(query);

          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_GetTotalSizeIsFast, 1);
        }


        // Installing this extension requires the "GlobalIntegers" table
        // created by the "FastTotalSize" extension
        property = 0;
        if (!LookupGlobalIntegerProperty(property, *db, t,
                                         Orthanc::GlobalProperty_HasFastCountResources) ||
            property != 1)
        {
          LOG(INFO) << "Installing the FastCountResources extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_FAST_COUNT_RESOURCES);
          db->Execute(query);

          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_HasFastCountResources, 1);
        }


        // Installing this extension requires the "GlobalIntegers" table
        // created by the "GetLastChangeIndex" extension
        property = 0;
        if (!LookupGlobalIntegerProperty(property, *db, t,
                                         Orthanc::GlobalProperty_GetLastChangeIndex) ||
            property != 1)
        {
          LOG(INFO) << "Installing the GetLastChangeIndex extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_GET_LAST_CHANGE_INDEX);
          db->Execute(query);

          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_GetLastChangeIndex, 1);
        }

        t.Commit();
      }
    }

    return db.release();
  }


  PostgreSQLIndex::PostgreSQLIndex(OrthancPluginContext* context,
                                   const PostgreSQLParameters& parameters) :
    IndexBackend(context, new Factory(*this)),
    parameters_(parameters),
    clearAll_(false)
  {
  }

  
  int64_t PostgreSQLIndex::CreateResource(const char* publicId,
                                          OrthancPluginResourceType type)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "INSERT INTO Resources VALUES(${}, ${type}, ${id}, NULL) RETURNING internalId");
     
    statement.SetParameterType("id", ValueType_Utf8String);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetUtf8Value("id", publicId);
    args.SetIntegerValue("type", static_cast<int>(type));
     
    statement.Execute(args);

    return ReadInteger64(statement, 0);
  }


  uint64_t PostgreSQLIndex::GetTotalCompressedSize()
  {
    // Fast version if extension "./FastTotalSize.sql" is installed
    uint64_t result;

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "SELECT value FROM GlobalIntegers WHERE key = 0");

      statement.SetReadOnly(true);
      statement.Execute();

      result = static_cast<uint64_t>(ReadInteger64(statement, 0));
    }
    
    assert(result == IndexBackend::GetTotalCompressedSize());
    return result;
  }

  
  uint64_t PostgreSQLIndex::GetTotalUncompressedSize()
  {
    // Fast version if extension "./FastTotalSize.sql" is installed
    uint64_t result;

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "SELECT value FROM GlobalIntegers WHERE key = 1");

      statement.SetReadOnly(true);
      statement.Execute();

      result = static_cast<uint64_t>(ReadInteger64(statement, 0));
    }
    
    assert(result == IndexBackend::GetTotalUncompressedSize());
    return result;
  }


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  void PostgreSQLIndex::CreateInstance(OrthancPluginCreateInstanceResult& result,
                                       const char* hashPatient,
                                       const char* hashStudy,
                                       const char* hashSeries,
                                       const char* hashInstance)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "SELECT * FROM CreateInstance(${patient}, ${study}, ${series}, ${instance})");

    statement.SetParameterType("patient", ValueType_Utf8String);
    statement.SetParameterType("study", ValueType_Utf8String);
    statement.SetParameterType("series", ValueType_Utf8String);
    statement.SetParameterType("instance", ValueType_Utf8String);

    Dictionary args;
    args.SetUtf8Value("patient", hashPatient);
    args.SetUtf8Value("study", hashStudy);
    args.SetUtf8Value("series", hashSeries);
    args.SetUtf8Value("instance", hashInstance);
    
    statement.Execute(args);

    if (statement.IsDone() ||
        statement.GetResultFieldsCount() != 8)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
    }

    for (size_t i = 0; i < 8; i++)
    {
      statement.SetResultFieldType(i, ValueType_Integer64);
    }

    result.isNewInstance = (ReadInteger64(statement, 3) == 1);
    result.instanceId = ReadInteger64(statement, 7);

    if (result.isNewInstance)
    {
      result.isNewPatient = (ReadInteger64(statement, 0) == 1);
      result.isNewStudy = (ReadInteger64(statement, 1) == 1);
      result.isNewSeries = (ReadInteger64(statement, 2) == 1);
      result.patientId = ReadInteger64(statement, 4);
      result.studyId = ReadInteger64(statement, 5);
      result.seriesId = ReadInteger64(statement, 6);
    }
  }
#endif


  uint64_t PostgreSQLIndex::GetResourceCount(OrthancPluginResourceType resourceType)
  {
    // Optimized version thanks to the "FastCountResources.sql" extension

    assert(OrthancPluginResourceType_Patient == 0 &&
           OrthancPluginResourceType_Study == 1 &&
           OrthancPluginResourceType_Series == 2 &&
           OrthancPluginResourceType_Instance == 3);

    uint64_t result;
    
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "SELECT value FROM GlobalIntegers WHERE key = ${key}");

      statement.SetParameterType("key", ValueType_Integer64);

      Dictionary args;

      // For an explanation of the "+ 2" below, check out "FastCountResources.sql"
      args.SetIntegerValue("key", static_cast<int>(resourceType + 2));

      statement.SetReadOnly(true);
      statement.Execute(args);

      result = static_cast<uint64_t>(ReadInteger64(statement, 0));
    }
      
    assert(result == IndexBackend::GetResourceCount(resourceType));
    return result;
  }


  int64_t PostgreSQLIndex::GetLastChangeIndex()
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "SELECT value FROM GlobalIntegers WHERE key = 6");

    statement.SetReadOnly(true);
    statement.Execute();

    return ReadInteger64(statement, 0);
  }


  void PostgreSQLIndex::TagMostRecentPatient(int64_t patient)
  {
    // This behavior is implemented in "CreateInstance()", and no
    // backward compatibility is necessary
    throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
  }
}
