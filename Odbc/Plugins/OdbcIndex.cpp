/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2025 Orthanc Team SRL, Belgium
 * Copyright (C) 2021-2025 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


#include "OdbcIndex.h"

#include "../../Framework/Common/Integer64Value.h"
#include "../../Framework/Odbc/OdbcDatabase.h"
#include "../../Framework/Plugins/GlobalProperties.h"

#include <EmbeddedResources.h>  // Autogenerated file

#include <Logging.h>
#include <OrthancException.h>
#include <Toolbox.h>

#include <boost/algorithm/string/replace.hpp>


// Some aliases for internal properties
static const Orthanc::GlobalProperty GlobalProperty_LastChange = Orthanc::GlobalProperty_DatabaseInternal0;


namespace OrthancDatabases
{
  static int64_t GetSQLiteLastInsert(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager, "SELECT LAST_INSERT_ROWID()");
    
    statement.Execute();
    
    return statement.ReadInteger64(0);
  }
  
  
  static int64_t GetMySQLLastInsert(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager, "SELECT LAST_INSERT_ID()");
    
    statement.Execute();
    
    return statement.ReadInteger64(0);
  }
  
  
  static int64_t GetMSSQLLastInsert(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager, "SELECT @@IDENTITY");
    
    statement.Execute();
    
    return statement.ReadInteger64(0);
  }
  
  
  static void AddPatientToRecyclingOrder(DatabaseManager& manager,
                                         int64_t patient)
  {
    // In the other database plugins, this is done with a trigger

    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    switch (manager.GetDialect())
    {
      case Dialect_SQLite:
      case Dialect_MySQL:
        statement.reset(
          new DatabaseManager::CachedStatement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO PatientRecyclingOrder VALUES(NULL, ${patient})"));
        break;
        
      case Dialect_PostgreSQL:
        statement.reset(
          new DatabaseManager::CachedStatement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO PatientRecyclingOrder VALUES(DEFAULT, ${patient})"));
        break;
        
      case Dialect_MSSQL:
        statement.reset(
          new DatabaseManager::CachedStatement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO PatientRecyclingOrder VALUES(${patient})"));
        break;
        
      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }

    statement->SetParameterType("patient", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("patient", patient);
    statement->Execute(args);
  }


  static OrthancPluginResourceType GetParentType(OrthancPluginResourceType level)
  {
    switch (level)
    {
      case OrthancPluginResourceType_Study:
        return OrthancPluginResourceType_Patient;

      case OrthancPluginResourceType_Series:
        return OrthancPluginResourceType_Study;

      case OrthancPluginResourceType_Instance:
        return OrthancPluginResourceType_Series;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }


  OdbcIndex::OdbcIndex(OrthancPluginContext* context,
                       const std::string& connectionString,
                       bool readOnly) :
    IndexBackend(context, readOnly),
    maxConnectionRetries_(10),
    connectionRetryInterval_(5),
    connectionString_(connectionString)
  {
  }

  
  void OdbcIndex::SetConnectionRetryInterval(unsigned int seconds)
  {
    if (seconds == 0)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
    else
    {
      connectionRetryInterval_ = seconds;
    }
  }


  IDatabaseFactory* OdbcIndex::CreateDatabaseFactory()
  {
    return OdbcDatabase::CreateDatabaseFactory(maxConnectionRetries_, connectionRetryInterval_, connectionString_, true);
  }
  
  
  void OdbcIndex::ConfigureDatabase(DatabaseManager& manager,
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

    OdbcDatabase& db = dynamic_cast<OdbcDatabase&>(manager.GetDatabase());

    if (!db.DoesTableExist("resources"))
    {
      std::string sql;
      Orthanc::EmbeddedResources::GetFileResource(sql, Orthanc::EmbeddedResources::ODBC_PREPARE_INDEX);

      switch (db.GetDialect())
      {
        case Dialect_SQLite:
          boost::replace_all(sql, "${LONGTEXT}", "TEXT");
          boost::replace_all(sql, "${AUTOINCREMENT_TYPE}", "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT");
          boost::replace_all(sql, "${AUTOINCREMENT_INSERT}", "NULL, ");
          break;
        
        case Dialect_PostgreSQL:
          boost::replace_all(sql, "${LONGTEXT}", "TEXT");
          boost::replace_all(sql, "${AUTOINCREMENT_TYPE}", "BIGSERIAL NOT NULL PRIMARY KEY");
          boost::replace_all(sql, "${AUTOINCREMENT_INSERT}", "DEFAULT, ");
          break;
        
        case Dialect_MySQL:
          boost::replace_all(sql, "${LONGTEXT}", "LONGTEXT");
          boost::replace_all(sql, "${AUTOINCREMENT_TYPE}", "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY");
          boost::replace_all(sql, "${AUTOINCREMENT_INSERT}", "NULL, ");
          break;

        case Dialect_MSSQL:
          /**
           * cf. OMSSQL-5: Use VARCHAR(MAX) instead of TEXT: (1)
           * Microsoft issued a warning stating that "ntext, text, and
           * image data types will be removed in a future version of
           * SQL Server"
           * (https://msdn.microsoft.com/en-us/library/ms187993.aspx),
           * and (2) SQL Server does not support comparison of TEXT
           * with '=' operator (e.g. in WHERE statements such as
           * IndexBackend::LookupIdentifier())."
           **/
          boost::replace_all(sql, "${LONGTEXT}", "VARCHAR(MAX)");
          boost::replace_all(sql, "${AUTOINCREMENT_TYPE}", "BIGINT IDENTITY NOT NULL PRIMARY KEY");
          boost::replace_all(sql, "${AUTOINCREMENT_INSERT}", "");
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
      }

      {
        DatabaseManager::Transaction t(manager, TransactionType_ReadWrite);

        db.ExecuteMultiLines(sql);

        if (db.GetDialect() == Dialect_MySQL)
        {
          // Switch to the collation that is the default since MySQL
          // 8.0.1. This must be *after* the creation of the tables.
          db.ExecuteMultiLines("ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        }

        t.Commit();
      }
    }
  }

  
  int64_t OdbcIndex::CreateResource(DatabaseManager& manager,
                                    const char* publicId,
                                    OrthancPluginResourceType type)
  {
    Dictionary args;
    args.SetUtf8Value("id", publicId);
    args.SetIntegerValue("type", static_cast<int>(type));
    
    switch (manager.GetDatabase().GetDialect())
    {
      case Dialect_SQLite:
      {
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO Resources VALUES(NULL, ${type}, ${id}, NULL)");
          
          statement.SetParameterType("id", ValueType_Utf8String);
          statement.SetParameterType("type", ValueType_Integer64);
          statement.Execute(args);
        }

        // Must be out of the scope of "DatabaseManager::CachedStatement"
        const int64_t id = GetSQLiteLastInsert(manager);
        
        if (type == OrthancPluginResourceType_Patient)
        {
          AddPatientToRecyclingOrder(manager, id);
        }
        
        return id;
      }
      
      case Dialect_PostgreSQL:
      {
        int64_t id;
        
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager,
            "INSERT INTO Resources VALUES(DEFAULT, ${type}, ${id}, NULL) RETURNING internalId");
          
          statement.SetParameterType("id", ValueType_Utf8String);
          statement.SetParameterType("type", ValueType_Integer64);
          statement.Execute(args);
          id = statement.ReadInteger64(0);
        }
        
        if (type == OrthancPluginResourceType_Patient)
        {
          AddPatientToRecyclingOrder(manager, id);
        }
        
        return id;
      }
        
      case Dialect_MySQL:
      {
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO Resources VALUES(NULL, ${type}, ${id}, NULL)");
          
          statement.SetParameterType("id", ValueType_Utf8String);
          statement.SetParameterType("type", ValueType_Integer64);
          statement.Execute(args);
        }
        
        // Must be out of the scope of "DatabaseManager::CachedStatement"
        const int64_t id = GetMySQLLastInsert(manager);
        
        if (type == OrthancPluginResourceType_Patient)
        {
          AddPatientToRecyclingOrder(manager, id);
        }

        return id;
      }
        
      case Dialect_MSSQL:
      {
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager, "INSERT INTO Resources VALUES(${type}, ${id}, NULL)");
          
          statement.SetParameterType("id", ValueType_Utf8String);
          statement.SetParameterType("type", ValueType_Integer64);
          statement.Execute(args);
        }
        
        // Must be out of the scope of "DatabaseManager::CachedStatement"
        const int64_t id = GetMSSQLLastInsert(manager);

        if (type == OrthancPluginResourceType_Patient)
        {
          AddPatientToRecyclingOrder(manager, id);
        }

        return id;
      }

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }
  }


  void OdbcIndex::DeleteResource(IDatabaseBackendOutput& output,
                                 DatabaseManager& manager,
                                 int64_t id)
  {
    /**
     * Contrarily to PostgreSQL and SQLite, the MySQL dialect
     * doesn't support cascaded delete inside the same
     * table. Furthermore, for maximum portability, we don't use
     * triggers in the ODBC plugins. We therefore implement a custom
     * version of this deletion.
     **/

    ClearDeletedFiles(manager);
    ClearDeletedResources(manager);

    OrthancPluginResourceType type;
    bool hasParent;
    int64_t parentId = 0;

    {
      DatabaseManager::CachedStatement lookupResource(
        STATEMENT_FROM_HERE, manager,
        "SELECT resourceType, parentId FROM Resources WHERE internalId=${id}");
      lookupResource.SetParameterType("id", ValueType_Integer64);
      
      Dictionary args;
      args.SetIntegerValue("id", id);
      lookupResource.Execute(args);
      
      if (lookupResource.IsDone())
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_UnknownResource);
      }
      
      type = static_cast<OrthancPluginResourceType>(lookupResource.ReadInteger32(0));
      
      if (lookupResource.GetResultField(1).GetType() == ValueType_Null)
      {
        hasParent = false;
      }
      else
      {
        hasParent = true;
        parentId = lookupResource.ReadInteger64(1);
      }
    }

    {
      DatabaseManager::CachedStatement scheduleRootDeletion(
        STATEMENT_FROM_HERE, manager,
        "INSERT INTO DeletedResources SELECT internalId, resourceType, publicId "
        "FROM Resources WHERE Resources.internalId = ${id}");
      scheduleRootDeletion.SetParameterType("id", ValueType_Integer64);
      
      Dictionary args;
      args.SetIntegerValue("id", id);
      scheduleRootDeletion.Execute(args);
    }

    {
      const std::string scheduleChildrenDeletion =
        "INSERT INTO DeletedResources SELECT Resources.internalId, Resources.resourceType, Resources.publicId "
        "FROM Resources INNER JOIN DeletedResources ON Resources.parentId = DeletedResources.internalId "
        "WHERE Resources.resourceType = ${level}";
      
      switch (type)
      {
        /**
         * WARNING: Don't add "break" or reorder cases below.
         **/
        
        case OrthancPluginResourceType_Patient:
        {
          DatabaseManager::CachedStatement statement(STATEMENT_FROM_HERE, manager, scheduleChildrenDeletion);
          statement.SetParameterType("level", ValueType_Integer64);
          
          Dictionary args;
          args.SetIntegerValue("level", OrthancPluginResourceType_Study);
          statement.Execute(args);
        }
        
        case OrthancPluginResourceType_Study:
        {
          DatabaseManager::CachedStatement statement(STATEMENT_FROM_HERE, manager, scheduleChildrenDeletion);
          statement.SetParameterType("level", ValueType_Integer64);
          
          Dictionary args;
          args.SetIntegerValue("level", OrthancPluginResourceType_Series);
          statement.Execute(args);
        }
        
        case OrthancPluginResourceType_Series:
        {
          DatabaseManager::CachedStatement statement(STATEMENT_FROM_HERE, manager, scheduleChildrenDeletion);
          statement.SetParameterType("level", ValueType_Integer64);

          Dictionary args;
          args.SetIntegerValue("level", OrthancPluginResourceType_Instance);
          statement.Execute(args);
        }
        
        case OrthancPluginResourceType_Instance:
          // No child
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
    }

    bool hasRemainingAncestor = false;
    std::string remainingAncestor;
    OrthancPluginResourceType ancestorType = OrthancPluginResourceType_None;
    
    if (hasParent)
    {
      int64_t currentAncestor = parentId;
      int64_t currentResource = id;
      OrthancPluginResourceType currentType = type;

      for (;;)
      {
        bool hasSiblings;

        {
          std::string suffix;
          if (manager.GetDialect() == Dialect_MSSQL)
          {
            suffix = "ORDER BY internalId OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY";
          }
          else
          {
            suffix = "LIMIT 1";
          }

          DatabaseManager::CachedStatement lookupSiblings(
            STATEMENT_FROM_HERE, manager,
            "SELECT internalId FROM Resources WHERE parentId = ${parent} AND internalId <> ${id} " + suffix);

          lookupSiblings.SetParameterType("parent", ValueType_Integer64);
          lookupSiblings.SetParameterType("id", ValueType_Integer64);

          Dictionary args;
          args.SetIntegerValue("parent", currentAncestor);
          args.SetIntegerValue("id", currentResource);
          lookupSiblings.Execute(args);

          hasSiblings = !lookupSiblings.IsDone();
        }

        if (hasSiblings)
        {
          // There remains some sibling: Signal this remaining ancestor
          hasRemainingAncestor = true;
          remainingAncestor = GetPublicId(manager, currentAncestor);
          ancestorType = GetParentType(currentType);
          break;
        }
        else
        {
          // No sibling remaining: This parent resource must be deleted
          {
            DatabaseManager::CachedStatement addDeletedResource(
              STATEMENT_FROM_HERE, manager,
              "INSERT INTO DeletedResources SELECT internalId, resourceType, publicId "
              "FROM Resources WHERE internalId=${id}");
            addDeletedResource.SetParameterType("id", ValueType_Integer64);

            Dictionary args;
            args.SetIntegerValue("id", currentAncestor);
            addDeletedResource.Execute(args);
          }

          int64_t tmp;
          if (LookupParent(tmp, manager, currentAncestor))
          {
            currentResource = currentAncestor;
            currentAncestor = tmp;
            currentType = GetParentType(currentType);
          }
          else
          {
            assert(currentType == OrthancPluginResourceType_Study);
            break;
          }
        }
      }
    }

    {
      // This is implemented by triggers in the PostgreSQL and MySQL plugins
      DatabaseManager::CachedStatement lookupDeletedAttachments(
        STATEMENT_FROM_HERE, manager,
        "INSERT INTO DeletedFiles SELECT AttachedFiles.* FROM AttachedFiles "
        "INNER JOIN DeletedResources ON AttachedFiles.id = DeletedResources.internalId");
      lookupDeletedAttachments.Execute();
    }

    {
      // Note that the attachments are automatically deleted by DELETE CASCADE
      DatabaseManager::CachedStatement applyResourcesDeletion(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM Resources WHERE internalId IN (SELECT internalId FROM DeletedResources)");
      applyResourcesDeletion.Execute();
    }

    SignalDeletedResources(output, manager);
    SignalDeletedFiles(output, manager);
    
    if (hasRemainingAncestor)
    {
      assert(!remainingAncestor.empty());
      output.SignalRemainingAncestor(remainingAncestor, ancestorType);
    }
  }


  static void ExecuteLogChange(DatabaseManager::CachedStatement& statement,
                               const Dictionary& args)
  {
    statement.SetParameterType("changeType", ValueType_Integer64);
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("resourceType", ValueType_Integer64);
    statement.SetParameterType("date", ValueType_Utf8String);
    statement.Execute(args);
  }
  
  
  void OdbcIndex::LogChange(DatabaseManager& manager,
                            int32_t changeType,
                            int64_t resourceId,
                            OrthancPluginResourceType resourceType,
                            const char* date)
  {
    Dictionary args;
    args.SetIntegerValue("changeType", changeType);
    args.SetIntegerValue("id", resourceId);
    args.SetIntegerValue("resourceType", resourceType);
    args.SetUtf8Value("date", date);

    int64_t seq;

    switch (manager.GetDatabase().GetDialect())
    {
      case Dialect_SQLite:
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO Changes VALUES(NULL, ${changeType}, ${id}, ${resourceType}, ${date})");
        ExecuteLogChange(statement, args);
        seq = GetSQLiteLastInsert(manager);
        break;
      }
      
      case Dialect_PostgreSQL:
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO Changes VALUES(DEFAULT, ${changeType}, ${id}, ${resourceType}, ${date}) RETURNING seq");
        ExecuteLogChange(statement, args);
        seq = statement.ReadInteger64(0);
        break;
      }
        
      case Dialect_MySQL:
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO Changes VALUES(NULL, ${changeType}, ${id}, ${resourceType}, ${date})");
        ExecuteLogChange(statement, args);
        seq = GetMySQLLastInsert(manager);
        break;
      }
        
      case Dialect_MSSQL:
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO Changes VALUES(${changeType}, ${id}, ${resourceType}, ${date})");
        ExecuteLogChange(statement, args);
        seq = GetMSSQLLastInsert(manager);
        break;
      }

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }
    
    std::string value = boost::lexical_cast<std::string>(seq);
    SetGlobalProperty(manager, MISSING_SERVER_IDENTIFIER, GlobalProperty_LastChange, value.c_str());
  }


  int64_t OdbcIndex::GetLastChangeIndex(DatabaseManager& manager)
  {
    std::string value;
    
    if (LookupGlobalProperty(value, manager, MISSING_SERVER_IDENTIFIER, GlobalProperty_LastChange))
    {
      return boost::lexical_cast<int64_t>(value);
    }
    else
    {
      return 0;
    }
  }


  void OdbcIndex::DeleteAttachment(IDatabaseBackendOutput& output,
                                   DatabaseManager& manager,
                                   int64_t id,
                                   int32_t attachment)
  {
    ClearDeletedFiles(manager);

    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("type", static_cast<int>(attachment));
    
    {
      // This is implemented by triggers in the PostgreSQL and MySQL plugins
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "INSERT INTO DeletedFiles SELECT * FROM AttachedFiles WHERE id=${id} AND fileType=${type}");
      
      statement.SetParameterType("id", ValueType_Integer64);
      statement.SetParameterType("type", ValueType_Integer64);
      statement.Execute(args);
    }

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM AttachedFiles WHERE id=${id} AND fileType=${type}");

      statement.SetParameterType("id", ValueType_Integer64);
      statement.SetParameterType("type", ValueType_Integer64);
      statement.Execute(args);
    }

    SignalDeletedFiles(output, manager);
  }


#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 12, 5)
  bool OdbcIndex::HasFindSupport() const
  {
    return false;
  }
#endif

#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 12, 5)
  void SQLiteIndex::ExecuteFind(Orthanc::DatabasePluginMessages::TransactionResponse& response,
                                DatabaseManager& manager,
                                const Orthanc::DatabasePluginMessages::Find_Request& request)
  {
    throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
  }

  void SQLiteIndex::ExecuteCount(Orthanc::DatabasePluginMessages::TransactionResponse& response,
                                 DatabaseManager& manager,
                                 const Orthanc::DatabasePluginMessages::Find_Request& request)
  {
    throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
  }
#endif
}
