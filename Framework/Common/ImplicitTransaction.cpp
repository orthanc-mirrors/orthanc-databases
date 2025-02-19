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


#include "ImplicitTransaction.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>

#include <memory>

namespace OrthancDatabases
{
  static bool isErrorOnDoubleExecution_ = false;
  
  
  ImplicitTransaction::ImplicitTransaction() :
    state_(State_Ready)
  {
  }

  
  ImplicitTransaction::~ImplicitTransaction()
  {
    switch (state_)
    {
      case State_Committed:
      case State_Ready:
        break;

      case State_Executed:
        LOG(ERROR) << "An implicit transaction has not been committed";
        break;

      default:
        LOG(ERROR) << "Internal error in ImplicitTransaction destructor";
        break;
    }
  }

  
  void ImplicitTransaction::Rollback()
  {
    LOG(ERROR) << "Cannot rollback an implicit transaction";
    throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
  }

  
  void ImplicitTransaction::Commit()
  {
    switch (state_)
    {
      case State_Ready:
        LOG(ERROR) << "Cannot commit an implicit transaction that has not been executed yet";
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);

      case State_Executed:
        state_ = State_Committed;
        break;

      case State_Committed:
        LOG(ERROR) << "Cannot commit twice an implicit transaction";
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);          
    }
  }


  void ImplicitTransaction::CheckStateForExecution()
  {
    switch (state_)
    {
      case State_Ready:
        // OK
        break;

      case State_Executed:
        if (isErrorOnDoubleExecution_)
        {
          /**
           * This allows to detect errors wrt. the handling of
           * transactions in the Orthanc core.
           *
           * In Orthanc <= 1.3.2: problems in "/changes" (a transaction
           * was missing because of GetPublicId()).
           **/
          LOG(ERROR) << "Cannot execute more than one statement in an implicit transaction";
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }
        
        break;        
          
      case State_Committed:
        LOG(ERROR) << "Cannot commit twice an implicit transaction";
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);          
    }
  }


  IResult* ImplicitTransaction::Execute(IPrecompiledStatement& statement,
                                        const Dictionary& parameters)
  {
    CheckStateForExecution();    
    std::unique_ptr<IResult> result(ExecuteInternal(statement, parameters));

    state_ = State_Executed;
    return result.release();
  }
  

  void ImplicitTransaction::ExecuteWithoutResult(IPrecompiledStatement& statement,
                                                 const Dictionary& parameters)
  {
    CheckStateForExecution();    
    ExecuteWithoutResultInternal(statement, parameters);

    state_ = State_Executed;
  }


  void ImplicitTransaction::SetErrorOnDoubleExecution(bool isError)
  {
    isErrorOnDoubleExecution_ = isError;
  }


  bool ImplicitTransaction::IsErrorOnDoubleExecution()
  {
    return isErrorOnDoubleExecution_;
  }
}

