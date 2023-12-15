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


#include "PostgreSQLIndex.h"

#include "../../Framework/Plugins/GlobalProperties.h"
#include "../../Framework/PostgreSQL/PostgreSQLDatabase.h"
#include "../../Framework/PostgreSQL/PostgreSQLTransaction.h"
#include "PostgreSQLDefinitions.h"

#include <EmbeddedResources.h>  // Auto-generated file

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Toolbox.h>
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
  PostgreSQLIndex::PostgreSQLIndex(OrthancPluginContext* context,
                                   const PostgreSQLParameters& parameters) :
    IndexBackend(context),
    parameters_(parameters),
    clearAll_(false)
  {
  }

  
  IDatabaseFactory* PostgreSQLIndex::CreateDatabaseFactory()
  {
    return PostgreSQLDatabase::CreateDatabaseFactory(parameters_);
  }

  
  void PostgreSQLIndex::ConfigureDatabase(DatabaseManager& manager,
                                          bool hasIdentifierTags,
                                          const std::list<IdentifierTag>& identifierTags)
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

    PostgreSQLDatabase& db = dynamic_cast<PostgreSQLDatabase&>(manager.GetDatabase());

    if (parameters_.HasLock())
    {
      db.AdvisoryLock(POSTGRESQL_LOCK_INDEX);
    }

    {
      PostgreSQLDatabase::TransientAdvisoryLock lock(db, POSTGRESQL_LOCK_DATABASE_SETUP);
      bool shouldInstallFastTotalStats2 = false;

      if (clearAll_)
      {
        db.ClearAll();
      }

      {
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        if (!t.GetDatabaseTransaction().DoesTableExist("Resources"))
        {
          std::string query;

          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_PREPARE_INDEX);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);

          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseSchemaVersion, expectedVersion);
          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabasePatchLevel, 1);
          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_HasTrigramIndex, 0);
        }
          
        if (!t.GetDatabaseTransaction().DoesTableExist("Resources"))
        {
          LOG(ERROR) << "Corrupted PostgreSQL database";
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);        
        }

        int version = 0;
        if (!LookupGlobalIntegerProperty(version, manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseSchemaVersion) ||
            version != 6)
        {
          LOG(ERROR) << "PostgreSQL plugin is incompatible with database schema version: " << version;
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }

        int revision;
        if (!LookupGlobalIntegerProperty(revision, manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabasePatchLevel))
        {
          revision = 1;
          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabasePatchLevel, revision);
        }

        if (revision == 1)
        {
          {
            LOG(WARNING) << "PostgreSQL plugin: adding UNIQUE(publicId) constraint to the 'Resources' table ";
            t.GetDatabaseTransaction().ExecuteMultiLines("ALTER TABLE Resources ADD UNIQUE (publicId);");
          }

          {
            LOG(WARNING) << "PostgreSQL plugin: adding or replacing ResourceDeletedFunc";

            std::string query;
            Orthanc::EmbeddedResources::GetFileResource
              (query, Orthanc::EmbeddedResources::POSTGRESQL_RESOURCE_DELETED_FUNC);
            t.GetDatabaseTransaction().ExecuteMultiLines(query);
          }

          {
            LOG(WARNING) << "PostgreSQL plugin: adding or replacing InsertOrUpdateMetadata";

            std::string query;
            Orthanc::EmbeddedResources::GetFileResource
              (query, Orthanc::EmbeddedResources::POSTGRESQL_INSERT_UPDATE_METADATA);
            t.GetDatabaseTransaction().ExecuteMultiLines(query);
          }

          shouldInstallFastTotalStats2 = true;

          revision = 2;
          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabasePatchLevel, revision);
        }

        if (revision != 2)
        {
          LOG(ERROR) << "PostgreSQL plugin is incompatible with database schema revision: " << revision;
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }

        t.Commit();
      }

      {
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        int hasTrigram = 0;
        if (!LookupGlobalIntegerProperty(hasTrigram, manager, MISSING_SERVER_IDENTIFIER,
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
           * https://orthanc.uclouvain.be/bugs/show_bug.cgi?id=47
           **/
          try
          {
            // We've observed 9 minutes on DB with 100000 studies
            LOG(WARNING) << "Trying to enable trigram matching on the PostgreSQL database "
                         << "to speed up wildcard searches. This may take several minutes";

            t.GetDatabaseTransaction().ExecuteMultiLines(
              "CREATE EXTENSION IF NOT EXISTS pg_trgm; "
              "CREATE INDEX DicomIdentifiersIndexValues2 ON DicomIdentifiers USING gin(value gin_trgm_ops);");

            SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_HasTrigramIndex, 1);
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
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        int property = 0;
        if (!LookupGlobalIntegerProperty(property, manager, MISSING_SERVER_IDENTIFIER,
                                         Orthanc::GlobalProperty_HasCreateInstance) ||
            property != 3)
        {
          LOG(INFO) << "Installing the CreateInstance extension";

          if (property == 1)
          {
            // Drop older, experimental versions of this extension
            t.GetDatabaseTransaction().ExecuteMultiLines("DROP FUNCTION CreateInstance("
                                                         "IN patient TEXT, IN study TEXT, IN series TEXT, in instance TEXT)");
          }
        
          // property == 2 was a first version of the CreateInstance -> we need to replace it by the new one
          // property == 3 is a new version (in v5.2) with same signature and CREATE OR UPDATE 
          //  -> we can replace the previous one without deleting it
          //     and we can create it if it has never been created.
          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_CREATE_INSTANCE);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);

          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_HasCreateInstance, 3);
        }

        if (!LookupGlobalIntegerProperty(property, manager, MISSING_SERVER_IDENTIFIER,
                                         Orthanc::GlobalProperty_GetTotalSizeIsFast) ||
            property != 1)
        {
          LOG(INFO) << "Installing the FastTotalSize extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_FAST_TOTAL_SIZE);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);

          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_GetTotalSizeIsFast, 1);
        }


        // Installing this extension requires the "GlobalIntegers" table
        // created by the "FastTotalSize" extension
        property = 0;
        if (!LookupGlobalIntegerProperty(property, manager, MISSING_SERVER_IDENTIFIER,
                                         Orthanc::GlobalProperty_HasFastCountResources) ||
            property != 1)
        {
          LOG(INFO) << "Installing the FastCountResources extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_FAST_COUNT_RESOURCES);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);

          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_HasFastCountResources, 1);
        }


        // Installing this extension requires the "GlobalIntegers" table
        // created by the "GetLastChangeIndex" extension
        property = 0;
        if (!LookupGlobalIntegerProperty(property, manager, MISSING_SERVER_IDENTIFIER,
                                         Orthanc::GlobalProperty_GetLastChangeIndex) ||
            property != 1)
        {
          LOG(INFO) << "Installing the GetLastChangeIndex extension";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_GET_LAST_CHANGE_INDEX);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);

          SetGlobalIntegerProperty(manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_GetLastChangeIndex, 1);
        }

        if (shouldInstallFastTotalStats2)
        {
          LOG(WARNING) << "PostgreSQL plugin: installing FastTotalStats2 to replace FastTotalSize and FastCountResources";

          std::string query;
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::POSTGRESQL_FAST_TOTAL_STATS_2);
          t.GetDatabaseTransaction().ExecuteMultiLines(query);
        }

        t.Commit();
      }
 

      {
        // New in release 4.0 to deal with multiple writers
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        if (!t.GetDatabaseTransaction().DoesTableExist("ServerProperties"))
        {
          t.GetDatabaseTransaction().ExecuteMultiLines("CREATE TABLE ServerProperties(server VARCHAR(64) NOT NULL, "
                                                       "property INTEGER, value TEXT, PRIMARY KEY(server, property))");
        }

        /**
         * PostgreSQL 9.5: "Adding a column with a default requires
         * updating each row of the table (to store the new column
         * value). However, if no default is specified, PostgreSQL is
         * able to avoid the physical update." => We set no default
         * for performance (older entries will be NULL)
         * https://www.postgresql.org/docs/9.5/ddl-alter.html
         **/
        if (!db.DoesColumnExist("Metadata", "revision"))
        {
          t.GetDatabaseTransaction().ExecuteMultiLines("ALTER TABLE Metadata ADD COLUMN revision INTEGER");
        }

        if (!db.DoesColumnExist("AttachedFiles", "revision"))
        {
          t.GetDatabaseTransaction().ExecuteMultiLines("ALTER TABLE AttachedFiles ADD COLUMN revision INTEGER");
        }

        t.Commit();
      }
 

      {
        // New in release 5.0 to deal with labels
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        if (!t.GetDatabaseTransaction().DoesTableExist("Labels"))
        {
          t.GetDatabaseTransaction().ExecuteMultiLines(
            "CREATE TABLE Labels("
            "id BIGINT REFERENCES Resources(internalId) ON DELETE CASCADE,"
            "label TEXT, PRIMARY KEY(id, label));"
            "CREATE INDEX LabelsIndex1 ON LABELS(id);"
            "CREATE INDEX LabelsIndex2 ON LABELS(label);");
        }

        t.Commit();
      }
    }
  }


  int64_t PostgreSQLIndex::CreateResource(DatabaseManager& manager,
                                          const char* publicId,
                                          OrthancPluginResourceType type)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO Resources VALUES(DEFAULT, ${type}, ${id}, NULL) RETURNING internalId");
     
    statement.SetParameterType("id", ValueType_Utf8String);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetUtf8Value("id", publicId);
    args.SetIntegerValue("type", static_cast<int>(type));
     
    statement.Execute(args);

    return statement.ReadInteger64(0);
  }


  uint64_t PostgreSQLIndex::GetTotalCompressedSize(DatabaseManager& manager)
  {
    // Fast version if extension "./FastTotalSize.sql" is installed
    uint64_t result;

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT value FROM GlobalIntegers WHERE key = 0");

      statement.SetReadOnly(true);
      statement.Execute();

      result = static_cast<uint64_t>(statement.ReadInteger64(0));
    }
    
    // disabled because this is not alway true while transactions are being executed in READ COMITTED TRANSACTION.  This is however true when no files are being delete/added
    //assert(result == IndexBackend::GetTotalCompressedSize(manager));
    return result;
  }

  
  uint64_t PostgreSQLIndex::GetTotalUncompressedSize(DatabaseManager& manager)
  {
    // Fast version if extension "./FastTotalSize.sql" is installed
    uint64_t result;

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT value FROM GlobalIntegers WHERE key = 1");

      statement.SetReadOnly(true);
      statement.Execute();

      result = static_cast<uint64_t>(statement.ReadInteger64(0));
    }
    
    // disabled because this is not alway true while transactions are being executed in READ COMITTED TRANSACTION.  This is however true when no files are being delete/added
    // assert(result == IndexBackend::GetTotalUncompressedSize(manager));
    return result;
  }

  int64_t PostgreSQLIndex::IncrementGlobalProperty(DatabaseManager& manager,
                                                   const char* serverIdentifier,
                                                   int32_t property,
                                                   int64_t increment)
  {
    if (serverIdentifier == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else
    {
      if (strlen(serverIdentifier) == 0)
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO GlobalProperties (property, value) VALUES(${property}, ${increment}) "
          "  ON CONFLICT (property) DO UPDATE SET value = CAST(GlobalProperties.value AS BIGINT) + ${increment}"
          " RETURNING CAST(value AS BIGINT)");

        statement.SetParameterType("property", ValueType_Integer64);
        statement.SetParameterType("increment", ValueType_Integer64);

        Dictionary args;
        args.SetIntegerValue("property", property);
        args.SetIntegerValue("increment", increment);
        
        statement.Execute(args);

        return statement.ReadInteger64(0);
      }
      else
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO ServerProperties (server, property, value) VALUES(${server}, ${property}, ${increment}) "
          "  ON CONFLICT (server, property) DO UPDATE SET value = CAST(ServerProperties.value AS BIGINT) + ${increment}"
          " RETURNING CAST(value AS BIGINT)");

        statement.SetParameterType("server", ValueType_Utf8String);
        statement.SetParameterType("property", ValueType_Integer64);
        statement.SetParameterType("increment", ValueType_Integer64);

        Dictionary args;
        args.SetUtf8Value("server", serverIdentifier);
        args.SetIntegerValue("property", property);
        args.SetIntegerValue("increment", increment);
        
        statement.Execute(args);

        return statement.ReadInteger64(0);
      }
    }
  }

  void PostgreSQLIndex::UpdateAndGetStatistics(DatabaseManager& manager,
                                               int64_t& patientsCount,
                                               int64_t& studiesCount,
                                               int64_t& seriesCount,
                                               int64_t& instancesCount,
                                               int64_t& compressedSize,
                                               int64_t& uncompressedSize)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM UpdateStatistics()");

    statement.Execute();

    patientsCount = statement.ReadInteger64(0);
    studiesCount = statement.ReadInteger64(1);
    seriesCount = statement.ReadInteger64(2);
    instancesCount = statement.ReadInteger64(3);
    compressedSize = statement.ReadInteger64(4);
    uncompressedSize = statement.ReadInteger64(5);
  }

  void PostgreSQLIndex::ClearDeletedFiles(DatabaseManager& manager)
  {
    { // note: the temporary table lifespan is the session, not the transaction -> that's why we need the IF NOT EXISTS
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT CreateDeletedFilesTemporaryTable()"
        );
      statement.ExecuteWithoutResult();
    }

  }

  void PostgreSQLIndex::ClearDeletedResources(DatabaseManager& manager)
  {
    { // note: the temporary table lifespan is the session, not the transaction -> that's why we need the IF NOT EXISTS
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "CREATE TEMPORARY TABLE IF NOT EXISTS  DeletedResources("
        "resourceType INTEGER NOT NULL,"
        "publicId VARCHAR(64) NOT NULL"
        ");"
        );
      statement.Execute();
    }
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM DeletedResources;"
        );

      statement.Execute();
    }

  }

  void PostgreSQLIndex::ClearRemainingAncestor(DatabaseManager& manager)
  {
  }

  void PostgreSQLIndex::DeleteResource(IDatabaseBackendOutput& output,
                                       DatabaseManager& manager,
                                       int64_t id)
  {
    // clearing of temporary table is now implemented in the funcion DeleteResource
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM DeleteResource(${id})");

    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    statement.Execute(args);

    if (statement.IsDone() ||
        statement.GetResultFieldsCount() != 2)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
    }

    statement.SetResultFieldType(0, ValueType_Integer64);
    statement.SetResultFieldType(1, ValueType_Utf8String);

    if (!statement.IsNull(0))
    {
      output.SignalRemainingAncestor(
        statement.ReadString(1),
        static_cast<OrthancPluginResourceType>(statement.ReadInteger32(0)));
    }

    SignalDeletedFiles(output, manager);
    SignalDeletedResources(output, manager);
  }



#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  void PostgreSQLIndex::CreateInstance(OrthancPluginCreateInstanceResult& result,
                                       DatabaseManager& manager,
                                       const char* hashPatient,
                                       const char* hashStudy,
                                       const char* hashSeries,
                                       const char* hashInstance)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
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

    // LOG(INFO) << statement.ReadInteger64(0) << statement.ReadInteger64(1) << statement.ReadInteger64(2) << statement.ReadInteger64(3);

    result.isNewInstance = (statement.ReadInteger64(3) == 1);
    result.instanceId = statement.ReadInteger64(7);

    if (result.isNewInstance)
    {
      result.isNewPatient = (statement.ReadInteger64(0) == 1);
      result.isNewStudy = (statement.ReadInteger64(1) == 1);
      result.isNewSeries = (statement.ReadInteger64(2) == 1);
      result.patientId = statement.ReadInteger64(4);
      result.studyId = statement.ReadInteger64(5);
      result.seriesId = statement.ReadInteger64(6);
    }
  }
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static void ExecuteSetResourcesContentTags(
    DatabaseManager& manager,
    const std::string& table,
    const std::string& variablePrefix,
    uint32_t count,
    const OrthancPluginResourcesContentTags* tags)
  {
    std::string sql;
    Dictionary args;
    
    for (uint32_t i = 0; i < count; i++)
    {
      std::string name = variablePrefix + boost::lexical_cast<std::string>(i);

      args.SetUtf8Value(name, tags[i].value);
      
      std::string insert = ("(" + boost::lexical_cast<std::string>(tags[i].resource) + ", " +
                            boost::lexical_cast<std::string>(tags[i].group) + ", " +
                            boost::lexical_cast<std::string>(tags[i].element) + ", " +
                            "${" + name + "})");

      if (sql.empty())
      {
        sql = "INSERT INTO " + table + " VALUES " + insert;
      }
      else
      {
        sql += ", " + insert;
      }
    }

    if (!sql.empty())
    {
      DatabaseManager::StandaloneStatement statement(manager, sql);

      for (uint32_t i = 0; i < count; i++)
      {
        statement.SetParameterType(variablePrefix + boost::lexical_cast<std::string>(i),
                                   ValueType_Utf8String);
      }

      statement.Execute(args);
    }
  }
#endif
  

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static void ExecuteSetResourcesContentMetadata(
    DatabaseManager& manager,
    bool hasRevisionsSupport,
    uint32_t count,
    const OrthancPluginResourcesContentMetadata* metadata)
  {
    if (count < 1)
    {
      return;
    }

    std::vector<std::string> resourceIds;
    std::vector<std::string> metadataTypes;
    std::vector<std::string> metadataValues;
    std::vector<std::string> revisions;

    Dictionary args;
    
    for (uint32_t i = 0; i < count; i++)
    {
      std::string argName = "m" + boost::lexical_cast<std::string>(i);

      args.SetUtf8Value(argName, metadata[i].value);

      resourceIds.push_back(boost::lexical_cast<std::string>(metadata[i].resource));
      metadataTypes.push_back(boost::lexical_cast<std::string>(metadata[i].metadata));
      metadataValues.push_back("${" + argName + "}");
      revisions.push_back("0");
    }

    std::string joinedResourceIds;
    std::string joinedMetadataTypes;
    std::string joinedMetadataValues;
    std::string joinedRevisions;

    Orthanc::Toolbox::JoinStrings(joinedResourceIds, resourceIds, ",");
    Orthanc::Toolbox::JoinStrings(joinedMetadataTypes, metadataTypes, ",");
    Orthanc::Toolbox::JoinStrings(joinedMetadataValues, metadataValues, ",");
    Orthanc::Toolbox::JoinStrings(joinedRevisions, revisions, ",");

    std::string sql = std::string("SELECT InsertOrUpdateMetadata(ARRAY[") + 
                                  joinedResourceIds + "], ARRAY[" + 
                                  joinedMetadataTypes + "], ARRAY[" + 
                                  joinedMetadataValues + "], ARRAY[" + 
                                  joinedRevisions + "])";

    DatabaseManager::StandaloneStatement statement(manager, sql);

    for (uint32_t i = 0; i < count; i++)
    {
      statement.SetParameterType("m" + boost::lexical_cast<std::string>(i),
                                  ValueType_Utf8String);
    }

    statement.Execute(args);
  }
#endif


  void PostgreSQLIndex::SetResourcesContent(DatabaseManager& manager,
                                     uint32_t countIdentifierTags,
                                     const OrthancPluginResourcesContentTags* identifierTags,
                                     uint32_t countMainDicomTags,
                                     const OrthancPluginResourcesContentTags* mainDicomTags,
                                     uint32_t countMetadata,
                                     const OrthancPluginResourcesContentMetadata* metadata)
  {
    ExecuteSetResourcesContentTags(manager, "DicomIdentifiers", "i",
                                   countIdentifierTags, identifierTags);

    ExecuteSetResourcesContentTags(manager, "MainDicomTags", "t",
                                   countMainDicomTags, mainDicomTags);
    
    ExecuteSetResourcesContentMetadata(manager, HasRevisionsSupport(), countMetadata, metadata);

  }


  uint64_t PostgreSQLIndex::GetResourcesCount(DatabaseManager& manager,
                                              OrthancPluginResourceType resourceType)
  {
    // Optimized version thanks to the "FastCountResources.sql" extension

    assert(OrthancPluginResourceType_Patient == 0 &&
           OrthancPluginResourceType_Study == 1 &&
           OrthancPluginResourceType_Series == 2 &&
           OrthancPluginResourceType_Instance == 3);

    uint64_t result;
    
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT value FROM GlobalIntegers WHERE key = ${key}");

      statement.SetParameterType("key", ValueType_Integer64);

      Dictionary args;

      // For an explanation of the "+ 2" below, check out "FastCountResources.sql"
      args.SetIntegerValue("key", static_cast<int>(resourceType + 2));

      statement.SetReadOnly(true);
      statement.Execute(args);

      result = static_cast<uint64_t>(statement.ReadInteger64(0));
    }
      
    // disabled because this is not alway true while transactions are being executed in READ COMITTED TRANSACTION.  This is however true when no files are being delete/added
    assert(result == IndexBackend::GetResourcesCount(manager, resourceType));

    return result;
  }


  int64_t PostgreSQLIndex::GetLastChangeIndex(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT value FROM GlobalIntegers WHERE key = 6");

    statement.SetReadOnly(true);
    statement.Execute();

    return statement.ReadInteger64(0);
  }


  void PostgreSQLIndex::TagMostRecentPatient(DatabaseManager& manager,
                                             int64_t patient)
  {
    // This behavior is implemented in "CreateInstance()", and no
    // backward compatibility is necessary
    throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
  }
}
