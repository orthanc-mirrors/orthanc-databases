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


#include "MySQLIndex.h"

#include "../../Framework/Plugins/GlobalProperties.h"
#include "../../Framework/MySQL/MySQLDatabase.h"
#include "../../Framework/MySQL/MySQLTransaction.h"
#include "MySQLDefinitions.h"

#include <EmbeddedResources.h>  // Auto-generated file

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>

#include <ctype.h>

namespace OrthancDatabases
{
  static void ThrowCannotCreateTrigger()
  {
    LOG(ERROR) << "The MySQL user is not allowed to create triggers => 2 possible solutions:";
    LOG(ERROR) << "  1- Give the SUPER privilege to the MySQL database user, or";
    LOG(ERROR) << "  2- Run \"set global log_bin_trust_function_creators=1;\" as MySQL root user.";
    LOG(ERROR) << "Once you are done, drop and recreate the MySQL database";
    throw Orthanc::OrthancException(Orthanc::ErrorCode_Database,
                                    "Need to fix the MySQL permissions for \"CREATE TRIGGER\"");
  }
  
  IDatabase* MySQLIndex::OpenInternal()
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

    if (!MySQLDatabase::IsValidDatabaseIdentifier(parameters_.GetDatabase()))
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }

    if (clearAll_)
    {
      MySQLDatabase::ClearDatabase(parameters_);
    }
    
    std::unique_ptr<MySQLDatabase> db(new MySQLDatabase(parameters_));

    db->Open();
    db->Execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", false);

    {
      MySQLDatabase::TransientAdvisoryLock lock(*db, MYSQL_LOCK_DATABASE_SETUP);

      /**
       * In a first transaction, we create the tables. Such a
       * transaction cannot be rollback: "The CREATE TABLE statement
       * in InnoDB is processed as a single transaction. This means
       * that a ROLLBACK from the user does not undo CREATE TABLE
       * statements the user made during that transaction."
       * https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
       *
       * As a consequence, we delay the initial population of the
       * tables in a sequence of transactions below. This solves the
       * error message "MySQL plugin is incompatible with database
       * schema version: 0" that was reported in the forum:
       * https://groups.google.com/d/msg/orthanc-users/OCFFkm1qm0k/Mbroy8VWAQAJ
       **/      
      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);
        
        db->Execute("ALTER DATABASE " + parameters_.GetDatabase() + 
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", false);

        // This is the first table to be created
        if (!db->DoesTableExist(t, "GlobalProperties"))
        {
          std::string query;
          
          Orthanc::EmbeddedResources::GetFileResource
            (query, Orthanc::EmbeddedResources::MYSQL_PREPARE_INDEX);
          db->Execute(query, true);
        }

        t.Commit();
      }

      /**
       * This is the sequence of transactions that initially populate
       * the database. WARNING - As table creation cannot be rollback,
       * don't forget to add "IF NOT EXISTS" if some table must be
       * created below this point (in order to recover from failed
       * transaction).
       **/

      int version = 0;

      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);

        // This is the last table to be created
        if (!db->DoesTableExist(t, "PatientRecyclingOrder"))
        {
          LOG(ERROR) << "Corrupted MySQL database";
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);        
        }

        // This is the last item to be created
        if (!db->DoesTriggerExist(t, "PatientAdded"))
        {
          ThrowCannotCreateTrigger();
        }

        if (!LookupGlobalIntegerProperty(version, *db, t, Orthanc::GlobalProperty_DatabaseSchemaVersion))
        {
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabaseSchemaVersion, expectedVersion);
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, 1);
          version = expectedVersion;
        }

        if (version != 6)
        {
          LOG(ERROR) << "MySQL plugin is incompatible with database schema version: " << version;
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }

        t.Commit();
      }

      int revision = 0;

      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);

        if (!LookupGlobalIntegerProperty(revision, *db, t, Orthanc::GlobalProperty_DatabasePatchLevel))
        {
          revision = 1;
          SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);
        }

        t.Commit();
      }

      if (revision == 1)
      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);
        
        // The serialization of jobs as a global property can lead to
        // very long values => switch to the LONGTEXT type that can
        // store up to 4GB:
        // https://stackoverflow.com/a/13932834/881731
        db->Execute("ALTER TABLE GlobalProperties MODIFY value LONGTEXT", false);
        
        revision = 2;
        SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);

        t.Commit();
      }

      if (revision == 2)
      {        
        MySQLTransaction t(*db, TransactionType_ReadWrite);

        // Install the "GetLastChangeIndex" extension
        std::string query;

        Orthanc::EmbeddedResources::GetFileResource
          (query, Orthanc::EmbeddedResources::MYSQL_GET_LAST_CHANGE_INDEX);
        db->Execute(query, true);

        if (!db->DoesTriggerExist(t, "ChangeAdded"))
        {
          ThrowCannotCreateTrigger();
        }
        
        revision = 3;
        SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);

        t.Commit();
      }
      
      if (revision == 3)
      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);

        // Reconfiguration of "Metadata" from TEXT type (up to 64KB)
        // to the LONGTEXT type (up to 4GB). This might be important
        // for applications such as the Osimis Web viewer that stores
        // large amount of metadata.
        // http://book.orthanc-server.com/faq/features.html#central-registry-of-metadata-and-attachments
        db->Execute("ALTER TABLE Metadata MODIFY value LONGTEXT", false);
        
        revision = 4;
        SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);

        t.Commit();
      }
      
      if (revision == 4)
      {
        MySQLTransaction t(*db, TransactionType_ReadWrite);
        
        // Install the "CreateInstance" extension
        std::string query;
        
        Orthanc::EmbeddedResources::GetFileResource
          (query, Orthanc::EmbeddedResources::MYSQL_CREATE_INSTANCE);
        db->Execute(query, true);
        
        revision = 5;
        SetGlobalIntegerProperty(*db, t, Orthanc::GlobalProperty_DatabasePatchLevel, revision);

        t.Commit();
      }
      
      if (revision != 5)
      {
        LOG(ERROR) << "MySQL plugin is incompatible with database schema revision: " << revision;
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
      }
    }

    /**
     * WARNING: This lock must be acquired after
     * "MYSQL_LOCK_DATABASE_SETUP" is released. Indeed, in MySQL <
     * 5.7, it is impossible to acquire more than one lock at a time,
     * as calling "SELECT GET_LOCK()" releases all the
     * previously-acquired locks.
     * https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html
     **/
    if (parameters_.HasLock())
    {
      db->AdvisoryLock(MYSQL_LOCK_INDEX);
    }
          
    return db.release();
  }


  MySQLIndex::MySQLIndex(OrthancPluginContext* context,
                         const MySQLParameters& parameters) :
    IndexBackend(context, new Factory(*this)),
    parameters_(parameters),
    clearAll_(false)
  {
  }


  int64_t MySQLIndex::CreateResource(const char* publicId,
                                     OrthancPluginResourceType type)
  {
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "INSERT INTO Resources VALUES(${}, ${type}, ${id}, NULL)");
    
      statement.SetParameterType("id", ValueType_Utf8String);
      statement.SetParameterType("type", ValueType_Integer64);

      Dictionary args;
      args.SetUtf8Value("id", publicId);
      args.SetIntegerValue("type", static_cast<int>(type));
    
      statement.Execute(args);
    }

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "SELECT LAST_INSERT_ID()");

      statement.Execute();
      
      return ReadInteger64(statement, 0);
    }
  }


  void MySQLIndex::DeleteResource(IDatabaseBackendOutput& output,
                                  int64_t id)
  {
    ClearDeletedFiles();

    // Recursive exploration of resources to be deleted, from the "id"
    // resource to the top of the tree of resources
    
    bool done = false;

    while (!done)
    {
      int64_t parentId;
      
      {
        DatabaseManager::CachedStatement lookupSiblings(
          STATEMENT_FROM_HERE, GetManager(),
          "SELECT parentId FROM Resources "
          "WHERE parentId = (SELECT parentId FROM Resources WHERE internalId=${id});");

        lookupSiblings.SetParameterType("id", ValueType_Integer64);

        Dictionary args;
        args.SetIntegerValue("id", id);
    
        lookupSiblings.Execute(args);

        if (lookupSiblings.IsDone())
        {
          // "id" is a root node
          done = true;
        }
        else
        {
          parentId = ReadInteger64(lookupSiblings, 0);
          lookupSiblings.Next();

          if (lookupSiblings.IsDone())
          {
            // "id" has no sibling node, recursively remove
            done = false;
            id = parentId;
          }
          else
          {
            // "id" has at least one sibling node: the parent node is the remaining ancestor
            done = true;

            DatabaseManager::CachedStatement parent(
              STATEMENT_FROM_HERE, GetManager(),
              "SELECT publicId, resourceType FROM Resources WHERE internalId=${id};");
            
            parent.SetParameterType("id", ValueType_Integer64);

            Dictionary args;
            args.SetIntegerValue("id", parentId);
    
            parent.Execute(args);

            output.SignalRemainingAncestor(
              ReadString(parent, 0),
              static_cast<OrthancPluginResourceType>(ReadInteger32(parent, 1)));
          }
        }
      }
    }

    {
      DatabaseManager::CachedStatement deleteHierarchy(
        STATEMENT_FROM_HERE, GetManager(),
        "DELETE FROM Resources WHERE internalId IN (SELECT * FROM (SELECT internalId FROM Resources WHERE internalId=${id} OR parentId=${id} OR parentId IN (SELECT internalId FROM Resources WHERE parentId=${id}) OR parentId IN (SELECT internalId FROM Resources WHERE parentId IN (SELECT internalId FROM Resources WHERE parentId=${id}))) as t);");
      
      deleteHierarchy.SetParameterType("id", ValueType_Integer64);
      
      Dictionary args;
      args.SetIntegerValue("id", id);
    
      deleteHierarchy.Execute(args);
    }

    SignalDeletedFiles(output);
  }

  
  int64_t MySQLIndex::GetLastChangeIndex()
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "SELECT value FROM GlobalIntegers WHERE property = 0");
    
    statement.SetReadOnly(true);
    statement.Execute();

    return ReadInteger64(statement, 0);
  }


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  void MySQLIndex::CreateInstance(OrthancPluginCreateInstanceResult& result,
                                  const char* hashPatient,
                                  const char* hashStudy,
                                  const char* hashSeries,
                                  const char* hashInstance)
  {
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "CALL CreateInstance(${patient}, ${study}, ${series}, ${instance}, "
        "@isNewPatient, @isNewStudy, @isNewSeries, @isNewInstance, "
        "@patientKey, @studyKey, @seriesKey, @instanceKey)");

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

      if (!statement.IsDone())
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
      }
    }

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, GetManager(),
        "SELECT @isNewPatient, @isNewStudy, @isNewSeries, @isNewInstance, "
        "@patientKey, @studyKey, @seriesKey, @instanceKey");

      statement.Execute();

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
  }
#endif
}
