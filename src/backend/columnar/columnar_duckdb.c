/*-------------------------------------------------------------------------
 *
 * columnar_duckdb.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "duckdb.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "pg_version_compat.h"
#include "safe_lib.h"

#include "access/relation.h"
#include "access/tupdesc.h"
#include "catalog/namespace.h"
#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"
#include "distributed/argutils.h"
#include "executor/tstoreReceiver.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"


#define CITUS_TABLE_FUNCTION_NAME "citus"
#define DUCKDB_VECTOR_ASSIGN(Vector, Type, Offset, Value) \
	((Type *) Vector)[Offset] = Value;
#define DAYS_BETWEEN_1970_2000 (10957)
#define MICROS_PER_SEC (1000000L)
#define MICROS_PER_DAY (86400 * MICROS_PER_SEC)


/* represents a target type when casting from DuckDB to Postgres types */
typedef struct PostgresTypeInfo
{
	/* OID of the type */
	Oid typeId;

	/* function to parse the type */
	FmgrInfo inputFunction;
	Oid typioparam;
} PostgresTypeInfo;

typedef struct CitusTableFunctionBindData
{
	/* name of the table passed to the table function */
	char *tableName;

	/* the opened relation */
	Relation relation;

	/* whether the table is columnar */
	bool isColumnar;
} CitusTableFunctionBindData;

typedef struct CitusTableFunctionInitData
{
	/* number of columns in the result */
	int resultColumnCount;

	/* for each result column, contains the index of the source column */
	int *resultColumnMap;

	/* regular scan state (for non-columnar tables) */
	TableScanDesc tableScanState;
	TupleTableSlot *tupleTableSlot;

	/* columnar read state */
	ColumnarReadState *columnarReadState;

	/* tuple descriptor for the table */
	TupleDesc tableDescriptor;

	/* whether scan is done */
	bool isScanDone;
} CitusTableFunctionInitData;


static void RunDuckDBQuery(char *query, TupleDesc tupleDescriptor,
						   Tuplestorestate *tupleStore);
static Datum DuckDBResultDatum(duckdb_result *result, int columnIndex, int rowIndex,
							   PostgresTypeInfo *targetType);
static ReturnSetInfo * CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc);
static Tuplestorestate * SetupTuplestore(FunctionCallInfo fcinfo,
										 TupleDesc *tupleDescriptor);
static void CitusTableFunctionBind(duckdb_bind_info info);
static void CitusTableFunctionInit(duckdb_init_info info);
static void CitusTableFunctionExec(duckdb_function_info info,
								   duckdb_data_chunk output);
static void CitusTableFunctionExecColumnar(duckdb_function_info info,
										   duckdb_data_chunk output);
static void CitusTableFunctionExecRowBased(duckdb_function_info info,
										   duckdb_data_chunk output);
static void CitusTableScanner(duckdb_replacement_scan_info info,
							  const char *tableName, void *data);
static duckdb_type DuckDBTypeForOid(Oid typeId, int typeMod);
static void CastDatumToDuckDBVectorElement(Datum value, Oid typeId, duckdb_vector,
										   void *data, int offset);


PG_FUNCTION_INFO_V1(run_duckdb_query);


static bool IsDuckDBInitialized = false;
static duckdb_database DuckDB;
static duckdb_connection DuckDBConn;
static duckdb_table_function CitusTableFunction;
static double TimeSpentReading;
static double TimeSpentCasting;

/*
 * InitializeDuckDB initializes DuckDB for the current process.
 */
static void
InitializeDuckDB(void)
{
	if (IsDuckDBInitialized)
	{
		return;
	}

	char *errorMessage = NULL;
	if (duckdb_open_ext(NULL, &DuckDB, NULL, &errorMessage) == DuckDBError)
	{
		ereport(ERROR, (errmsg("could not start DuckDB: %s",
							   errorMessage != NULL ? errorMessage : "internal error")));
	}

	CitusTableFunction = duckdb_create_table_function();
	duckdb_table_function_set_name(CitusTableFunction, CITUS_TABLE_FUNCTION_NAME);
	duckdb_logical_type paramType = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_table_function_add_parameter(CitusTableFunction, paramType);
	duckdb_table_function_set_bind(CitusTableFunction, CitusTableFunctionBind);
	duckdb_table_function_set_init(CitusTableFunction, CitusTableFunctionInit);
	duckdb_table_function_set_function(CitusTableFunction, CitusTableFunctionExec);
	duckdb_table_function_supports_projection_pushdown(CitusTableFunction, true);

	void *extraData = NULL;
	duckdb_delete_callback_t deleteCallback = NULL;
	duckdb_add_replacement_scan(DuckDB, CitusTableScanner, (void *) extraData,
								deleteCallback);

	if (duckdb_connect(DuckDB, &DuckDBConn) == DuckDBError)
	{
		ereport(ERROR, (errmsg("could not connect to DuckDB")));
	}

	duckdb_state status = duckdb_register_table_function(DuckDBConn, CitusTableFunction);
	if (status == DuckDBError)
	{
		ereport(ERROR, (errmsg("registering table function failed")));
	}

	duckdb_result duckResult;
	status = duckdb_query(DuckDBConn, "SET threads TO 1", &duckResult);

	if (status == DuckDBError)
	{
		/* TODO: add a memory context hook to destroy result? */
		errorMessage = pstrdup(duckdb_result_error(&duckResult));
		duckdb_destroy_result(&duckResult);

		ereport(ERROR, (errmsg("initialization failed: %s", errorMessage)));
	}

	duckdb_destroy_result(&duckResult);

	IsDuckDBInitialized = true;
}


/*
 * run_duckdb_query runs a query via DuckDB.
 */
Datum
run_duckdb_query(PG_FUNCTION_ARGS)
{
	char *query = PG_GETARG_TEXT_TO_CSTRING(0);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	PG_TRY();
	{
		TimeSpentReading = 0.;
		TimeSpentCasting = 0.;

		RunDuckDBQuery(query, tupleDescriptor, tupleStore);

		ereport(DEBUG1, (errmsg("time in PG/Citus code: reading "
								"%fms, casting %fms, total %fms",
								TimeSpentReading, TimeSpentCasting,
								TimeSpentReading + TimeSpentCasting)));
	}
	PG_CATCH();
	{
		/* clean up in case of error (TODO: does this clean up everything?) */
		duckdb_close(&DuckDB);
		IsDuckDBInitialized = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_DATUM(0);
}


/*
 * RunDuckDBQuery runs the given query using DuckDB and writes the results
 * to the tuplestore.
 */
static void
RunDuckDBQuery(char *query, TupleDesc tupleDescriptor,
			   Tuplestorestate *tupleStore)
{
	duckdb_state duckState;

	InitializeDuckDB();

	duckdb_result duckResult;
	duckState = duckdb_query(DuckDBConn, query, &duckResult);

	/* TODO: add a memory context hook to destroy result? */

	if (duckState == DuckDBError)
	{
		char *errorMessage = pstrdup(duckdb_result_error(&duckResult));
		duckdb_destroy_result(&duckResult);

		ereport(ERROR, (errmsg("query failed: %s", errorMessage)));
	}

	/* TODO: consider using duckdb_column_data with custom casts */
	idx_t rowCount = duckdb_row_count(&duckResult);
	idx_t queryResultColumnCount = duckdb_column_count(&duckResult);

	int outputColumnCount = tupleDescriptor->natts;
	int usedColumnCount = Min(queryResultColumnCount, outputColumnCount);

	PostgresTypeInfo *targetTypes =
		(PostgresTypeInfo *) palloc(sizeof(PostgresTypeInfo) * outputColumnCount);

	for (idx_t columnIndex = 0; columnIndex < outputColumnCount; columnIndex++)
	{
		PostgresTypeInfo *targetType = &(targetTypes[columnIndex]);

		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		targetType->typeId = currentColumn->atttypid;

		Oid inputFuncId = InvalidOid;
		getTypeInputInfo(targetType->typeId, &inputFuncId,
						 &(targetType->typioparam));

		fmgr_info(inputFuncId, &(targetType->inputFunction));
	}

	Datum *values = palloc0(sizeof(Datum) * outputColumnCount);
	bool *nulls = palloc0(sizeof(bool) * outputColumnCount);

	/* output columns without a query result are NULL */
	for (idx_t columnIndex = usedColumnCount; columnIndex < outputColumnCount;
		 columnIndex++)
	{
		nulls[columnIndex] = true;
	}

	for (idx_t rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		for (idx_t columnIndex = 0; columnIndex < usedColumnCount; columnIndex++)
		{
			PostgresTypeInfo *targetType = &(targetTypes[columnIndex]);

			bool isNull = duckdb_value_is_null(&duckResult, columnIndex, rowIndex);
			nulls[columnIndex] = isNull;

			if (!isNull)
			{
				Datum value = DuckDBResultDatum(&duckResult, columnIndex, rowIndex,
												targetType);

				values[columnIndex] = value;
			}
		}

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, nulls);
	}

	/* TODO: do we leak memory in case of OOM above? */
	duckdb_destroy_result(&duckResult);
}


/*
 * DuckDBResultDatum returns a single value from a DuckDB result as a Datum.
 */
static Datum
DuckDBResultDatum(duckdb_result *result, int columnIndex, int rowIndex,
				  PostgresTypeInfo *targetType)
{
	switch (targetType->typeId)
	{
		case BOOLOID:
		{
			bool sourceValue = duckdb_value_boolean(result, columnIndex, rowIndex);
			return BoolGetDatum(sourceValue);
		}

		case FLOAT4OID:
		{
			float sourceValue = duckdb_value_float(result, columnIndex, rowIndex);
			return Float4GetDatum(sourceValue);
		}

		case FLOAT8OID:
		{
			double sourceValue = duckdb_value_float(result, columnIndex, rowIndex);
			return Float8GetDatum(sourceValue);
		}

		case NUMERICOID:
		{
			/* TODO: should consider decimals */
			int64_t sourceValue = duckdb_value_int64(result, columnIndex, rowIndex);
			return NumericGetDatum(int64_to_numeric(sourceValue));
		}

		case INT2OID:
		{
			int16_t sourceValue = duckdb_value_int16(result, columnIndex, rowIndex);
			return Int16GetDatum(sourceValue);
		}

		case INT4OID:
		{
			int32_t sourceValue = duckdb_value_int32(result, columnIndex, rowIndex);
			return Int32GetDatum(sourceValue);
		}

		case INT8OID:
		{
			int64_t sourceValue = duckdb_value_int64(result, columnIndex, rowIndex);
			return Int32GetDatum(sourceValue);
		}

		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCOLLATIONOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
		{
			int32_t sourceValue = duckdb_value_int32(result, columnIndex, rowIndex);
			return ObjectIdGetDatum(sourceValue);
		}

		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		{
			char *sourceValue = duckdb_value_varchar(result, columnIndex, rowIndex);

			/* copies the string into newly allocated memory */
			text *textValue = cstring_to_text(sourceValue);

			duckdb_free(sourceValue);

			return PointerGetDatum(textValue);
		}

		case BYTEAOID:
		{
			duckdb_blob blob = duckdb_value_blob(result, columnIndex, rowIndex);

			int datumSize = blob.size + VARHDRSZ;
			bytea *byteArray = (bytea *) palloc(datumSize);
			SET_VARSIZE(result, datumSize);

			memcpy_s(byteArray + VARHDRSZ, blob.size, blob.data, blob.size);

			/* TODO: do we leak memory in case of OOM above? */
			duckdb_free(blob.data);

			return PointerGetDatum(byteArray);
		}

		default:
		{
			char *sourceValue = duckdb_value_varchar(result, columnIndex, rowIndex);

			Datum outputValue = FunctionCall3(&(targetType->inputFunction),
											  CStringGetDatum(sourceValue),
											  targetType->typioparam,
											  Int32GetDatum(-1));

			/* TODO: do we leak memory in case of OOM above? */
			duckdb_free(sourceValue);
			return outputValue;
		}
	}
}


/*
 * CheckTuplestoreReturn checks if a tuplestore can be returned in the callsite
 * of the UDF.
 */
static ReturnSetInfo *
CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	ReturnSetInfo *returnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* check to see if caller supports us returning a tuplestore */
	if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot " \
						"accept a set")));
	}
	if (!(returnSetInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}
	switch (get_call_result_type(fcinfo, NULL, tupdesc))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		}

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
			break;
		}
	}
	return returnSetInfo;
}


/*
 * SetupTuplestore sets up a tuplestore for returning data.
 */
static Tuplestorestate *
SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupleDescriptor)
{
	ReturnSetInfo *resultSet = CheckTuplestoreReturn(fcinfo, tupleDescriptor);
	MemoryContext perQueryContext = resultSet->econtext->ecxt_per_query_memory;

	MemoryContext oldContext = MemoryContextSwitchTo(perQueryContext);
	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	resultSet->returnMode = SFRM_Materialize;
	resultSet->setResult = tupstore;
	resultSet->setDesc = *tupleDescriptor;
	MemoryContextSwitchTo(oldContext);

	return tupstore;
}


/*
 * CitusTableFunctionBind gets the values of parameters and decides
 * on the return type.
 */
static void
CitusTableFunctionBind(duckdb_bind_info info)
{
	duckdb_value tableNameParam = duckdb_bind_get_parameter(info, 0);
	char *tableName = duckdb_get_varchar(tableNameParam);

	List *nameList = stringToQualifiedNameList(tableName);
	RangeVar *rangeVar = makeRangeVarFromNameList(nameList);

	bool missingOk = true;
	Relation relation = relation_openrv_extended(rangeVar, AccessShareLock, missingOk);
	if (relation == NULL)
	{
		duckdb_bind_set_error(info, "table does not exist");
		return;
	}

	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDescriptor, columnIndex);
		char *columnName = NameStr(column->attname);

		duckdb_type duckType = DuckDBTypeForOid(column->atttypid, column->atttypmod);
		duckdb_logical_type logicalType = duckdb_create_logical_type(duckType);
		duckdb_bind_add_result_column(info, columnName, logicalType);
		duckdb_destroy_logical_type(&logicalType);
	}

	int bindDataSize = sizeof(CitusTableFunctionBindData);
	CitusTableFunctionBindData *bindData =
		(CitusTableFunctionBindData *) palloc0(bindDataSize);

	/* remember the table name */
	bindData->tableName = pstrdup(tableName);
	bindData->relation = relation;
	bindData->isColumnar = relation->rd_tableam == GetColumnarTableAmRoutine();

	duckdb_free(tableName);
	duckdb_destroy_value(&tableNameParam);

	duckdb_bind_set_bind_data(info, bindData, pfree);
}


/*
 * CitusTableFunctionInit initializes the state of a function call.
 */
static void
CitusTableFunctionInit(duckdb_init_info info)
{
	CitusTableFunctionBindData *bindData =
		(CitusTableFunctionBindData *) duckdb_init_get_bind_data(info);

	int resultColumnCount = duckdb_init_get_column_count(info);

	TupleDesc tableDescriptor = RelationGetDescr(bindData->relation);
	List *projectedColumnList = NIL;

	/* map result column index to source column index */
	int *resultColumnMap = (int *) palloc(sizeof(int) * resultColumnCount);

	for (int resultColumnIndex = 0; resultColumnIndex < resultColumnCount;
		 resultColumnIndex++)
	{
		int sourceColumnIndex = duckdb_init_get_column_index(info, resultColumnIndex);

		resultColumnMap[resultColumnIndex] = sourceColumnIndex;

		/* when only doing a count, source column index is -1 */
		if (sourceColumnIndex >= 0)
		{
			/* signal to columnar reader that we are interested in this column */
			projectedColumnList = lappend_int(projectedColumnList, sourceColumnIndex + 1);
		}
	}

	CitusTableFunctionInitData *initData =
		(CitusTableFunctionInitData *) palloc0(sizeof(CitusTableFunctionInitData));

	initData->resultColumnCount = resultColumnCount;
	initData->resultColumnMap = resultColumnMap;

	Snapshot snapshot = GetTransactionSnapshot();

	if (bindData->isColumnar)
	{
		/* TODO: use restrict info where possible */
		List *whereClauseList = NIL;
		MemoryContext scanContext = CurrentMemoryContext;
		bool randomAccess = false;

		initData->columnarReadState =
			ColumnarBeginRead(bindData->relation, tableDescriptor, projectedColumnList,
							  whereClauseList, scanContext, snapshot, randomAccess);
	}
	else
	{
		ScanKeyData *key = NULL;
		int numKeys = 0;

		initData->tableScanState = table_beginscan(bindData->relation, snapshot,
												   numKeys, key);
		initData->tupleTableSlot = table_slot_create(bindData->relation, NULL);
	}

	initData->tableDescriptor = tableDescriptor;

	duckdb_init_set_init_data(info, initData, pfree);

	/* underlying code to read from buffer manager is not thread safe */
	duckdb_init_set_max_threads(info, 1);

	/*duckdb_init_set_error(info, "My error message"); */
}


/*
 * CitusTableFunctionExec implements the table function.
 */
static void
CitusTableFunctionExec(duckdb_function_info info, duckdb_data_chunk output)
{
	CitusTableFunctionBindData *bindData =
		(CitusTableFunctionBindData *) duckdb_init_get_bind_data(info);

	if (bindData->isColumnar)
	{
		CitusTableFunctionExecColumnar(info, output);
	}
	else
	{
		CitusTableFunctionExecRowBased(info, output);
	}
}


/*
 * CitusTableFunctionExecColumnar implements the table function for columnar tables.
 */
static void
CitusTableFunctionExecColumnar(duckdb_function_info info, duckdb_data_chunk output)
{
	CitusTableFunctionBindData *bindData =
		(CitusTableFunctionBindData *) duckdb_init_get_bind_data(info);
	CitusTableFunctionInitData *initData =
		(CitusTableFunctionInitData *) duckdb_function_get_init_data(info);

	int resultColumnCount = initData->resultColumnCount;
	TupleDesc tableDescriptor = initData->tableDescriptor;
	ColumnarReadState *columnarReadState = initData->columnarReadState;

	if (initData->isScanDone)
	{
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	instr_time readStartTime;
	INSTR_TIME_SET_CURRENT(readStartTime);

	uint64 rowsAvailable = ColumnarReadNextVector(columnarReadState);

	instr_time readEndTime;
	INSTR_TIME_SET_CURRENT(readEndTime);
	INSTR_TIME_SUBTRACT(readEndTime, readStartTime);
	TimeSpentReading += INSTR_TIME_GET_MILLISEC(readEndTime);

	if (rowsAvailable == 0)
	{
		ColumnarEndRead(columnarReadState);
		relation_close(bindData->relation, NoLock);
		duckdb_data_chunk_set_size(output, 0);
		initData->isScanDone = true;
		return;
	}

	instr_time castStartTime;
	INSTR_TIME_SET_CURRENT(castStartTime);

	/* TODO: consider always filling up the vector */
	uint64 chunkSize = Min(duckdb_vector_size(), rowsAvailable);

	for (int resultColumnIndex = 0; resultColumnIndex < resultColumnCount;
		 resultColumnIndex++)
	{
		int sourceColumnIndex = initData->resultColumnMap[resultColumnIndex];
		if (sourceColumnIndex < 0)
		{
			/* only counting, setting chunk size is enough */
			continue;
		}

		Form_pg_attribute column = TupleDescAttr(tableDescriptor, sourceColumnIndex);
		Oid typeId = column->atttypid;

		duckdb_vector vector = duckdb_data_chunk_get_vector(output, resultColumnIndex);
		duckdb_vector_ensure_validity_writable(vector);
		uint64_t *validityMask = duckdb_vector_get_validity(vector);

		/* get Citus columnar vector pointers */
		Datum *valuesVector = NULL;
		bool *existsVector = NULL;
		GetColumnarVector(columnarReadState, sourceColumnIndex, &existsVector,
						  &valuesVector);

		bool doBatchCopy = false;
		void *data = duckdb_vector_get_data(vector);

		if (typeId == FLOAT8OID || typeId == INT8OID)
		{
			int datumSize = 8;
			doBatchCopy = true;

			/* when DuckDB and PG use the same format, copy the data into the vector */
			memcpy_s(data, chunkSize * datumSize, valuesVector, chunkSize * datumSize);
		}

		for (idx_t rowIndex = 0; rowIndex < chunkSize; rowIndex++)
		{
			bool exists = existsVector[rowIndex];

			if (exists && !doBatchCopy)
			{
				Datum value = valuesVector[rowIndex];
				CastDatumToDuckDBVectorElement(value, typeId, vector, data, rowIndex);
			}

			duckdb_validity_set_row_validity(validityMask, rowIndex, exists);
		}
	}

	instr_time castEndTime;
	INSTR_TIME_SET_CURRENT(castEndTime);
	INSTR_TIME_SUBTRACT(castEndTime, castStartTime);
	TimeSpentCasting += INSTR_TIME_GET_MILLISEC(castEndTime);

	ConsumeColumnarVector(columnarReadState, chunkSize);
	duckdb_data_chunk_set_size(output, chunkSize);

	/*duckdb_function_set_error(info, "My error message"); */
}


/*
 * CitusTableFunctionExecRowBased implements the table function for row-based tables.
 */
static void
CitusTableFunctionExecRowBased(duckdb_function_info info, duckdb_data_chunk output)
{
	CitusTableFunctionBindData *bindData =
		(CitusTableFunctionBindData *) duckdb_init_get_bind_data(info);
	CitusTableFunctionInitData *initData =
		(CitusTableFunctionInitData *) duckdb_function_get_init_data(info);

	int resultColumnCount = initData->resultColumnCount;
	TupleDesc tableDescriptor = initData->tableDescriptor;

	TableScanDesc tableScanState = initData->tableScanState;
	TupleTableSlot *tupleTableSlot = initData->tupleTableSlot;

	idx_t vectorSize = duckdb_vector_size();
	idx_t rowIndex = 0;

	/* make sure we can write validity mask */
	for (int resultColumnIndex = 0; resultColumnIndex < resultColumnCount;
		 resultColumnIndex++)
	{
		duckdb_vector vector = duckdb_data_chunk_get_vector(output, resultColumnIndex);
		duckdb_vector_ensure_validity_writable(vector);
	}

	while (!initData->isScanDone && rowIndex < vectorSize)
	{
		CHECK_FOR_INTERRUPTS();

		instr_time readStartTime;
		INSTR_TIME_SET_CURRENT(readStartTime);

		if (!table_scan_getnextslot(tableScanState, ForwardScanDirection, tupleTableSlot))
		{
			table_endscan(tableScanState);
			ExecDropSingleTupleTableSlot(tupleTableSlot);
			relation_close(bindData->relation, NoLock);

			initData->isScanDone = true;
			break;
		}

		slot_getallattrs(tupleTableSlot);

		instr_time readEndTime;
		INSTR_TIME_SET_CURRENT(readEndTime);
		INSTR_TIME_SUBTRACT(readEndTime, readStartTime);
		TimeSpentReading += INSTR_TIME_GET_MILLISEC(readEndTime);

		Datum *columnValues = tupleTableSlot->tts_values;
		bool *columnNulls = tupleTableSlot->tts_isnull;

		instr_time castStartTime;
		INSTR_TIME_SET_CURRENT(castStartTime);

		for (int resultColumnIndex = 0; resultColumnIndex < resultColumnCount;
			 resultColumnIndex++)
		{
			int sourceColumnIndex = initData->resultColumnMap[resultColumnIndex];
			if (sourceColumnIndex < 0)
			{
				/* only counting, setting chunk size is enough */
				continue;
			}

			Form_pg_attribute column = TupleDescAttr(tableDescriptor, sourceColumnIndex);
			Oid typeId = column->atttypid;

			duckdb_vector vector = duckdb_data_chunk_get_vector(output,
																resultColumnIndex);
			uint64_t *validityMask = duckdb_vector_get_validity(vector);

			Datum value = columnValues[sourceColumnIndex];
			bool isNull = columnNulls[sourceColumnIndex];

			void *data = duckdb_vector_get_data(vector);

			if (!isNull)
			{
				CastDatumToDuckDBVectorElement(value, typeId, vector, data, rowIndex);
			}

			duckdb_validity_set_row_validity(validityMask, rowIndex, !isNull);
		}

		instr_time castEndTime;
		INSTR_TIME_SET_CURRENT(castEndTime);
		INSTR_TIME_SUBTRACT(castEndTime, castStartTime);
		TimeSpentCasting += INSTR_TIME_GET_MILLISEC(castEndTime);

		rowIndex++;
	}

	duckdb_data_chunk_set_size(output, rowIndex);
}


/*
 * CitusTableScanner is a callback for initializing a replacement scan that
 * handles tables.
 */
static void
CitusTableScanner(duckdb_replacement_scan_info info, const char *tableName, void *data)
{
	duckdb_replacement_scan_set_function_name(info, CITUS_TABLE_FUNCTION_NAME);
	duckdb_value tableNameValue = duckdb_create_varchar(tableName);
	duckdb_replacement_scan_add_parameter(info, tableNameValue);
	duckdb_destroy_value(&tableNameValue);
}


/*
 * DuckDBTypeForOid converts a type OID to a DuckDB type.
 */
static duckdb_type
DuckDBTypeForOid(Oid typeId, int typeMod)
{
	/* TODO: handle array, enum */

	switch (typeId)
	{
		case BOOLOID:
		{
			return DUCKDB_TYPE_BOOLEAN;
		}

		case FLOAT4OID:
		{
			return DUCKDB_TYPE_FLOAT;
		}

		case FLOAT8OID:
		{
			return DUCKDB_TYPE_DOUBLE;
		}

		case NUMERICOID:
		{
			if (typeMod == -1)
			{
				return DUCKDB_TYPE_DOUBLE;
			}

			return DUCKDB_TYPE_DECIMAL;
		}

		case INT2OID:
		{
			return DUCKDB_TYPE_SMALLINT;
		}

		case INT4OID:
		{
			return DUCKDB_TYPE_INTEGER;
		}

		case INT8OID:
		{
			return DUCKDB_TYPE_BIGINT;
		}

		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCOLLATIONOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
		{
			return DUCKDB_TYPE_UINTEGER;
		}

		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		{
			return DUCKDB_TYPE_VARCHAR;
		}

		case JSONBOID:
		{
			/* maps to varchar, but with different conversion */
			return DUCKDB_TYPE_VARCHAR;
		}

		case BYTEAOID:
		{
			return DUCKDB_TYPE_BLOB;
		}

		case DATEOID:
		{
			return DUCKDB_TYPE_DATE;
		}

		case TIMEOID:
		{
			return DUCKDB_TYPE_TIME;
		}

		case TIMETZOID:
		{
			return DUCKDB_TYPE_TIME;
		}

		case TIMESTAMPOID:
		{
			return DUCKDB_TYPE_TIMESTAMP;
		}

		case TIMESTAMPTZOID:
		{
			return DUCKDB_TYPE_TIMESTAMP;
		}

		case INTERVALOID:
		{
			return DUCKDB_TYPE_INTERVAL;
		}

		case UUIDOID:
		{
			return DUCKDB_TYPE_UUID;
		}

		default:
		{
			return DUCKDB_TYPE_INVALID;
		}
	}
}


/*
 * CastDatumToDuckDBVectorElement converts a Postgres datum to a DuckDB
 * vector value.
 *
 * The type that we write with the factor should always be consistent
 * with DuckDBTypeForOid, since that will be the type that DuckDB expects.
 */
static void
CastDatumToDuckDBVectorElement(Datum datum, Oid typeId, duckdb_vector vector,
							   void *data, int offset)
{
	switch (typeId)
	{
		case BOOLOID:
		{
			bool boolValue = DatumGetBool(datum);
			DUCKDB_VECTOR_ASSIGN(data, bool, offset, boolValue);
			break;
		}

		case FLOAT4OID:
		{
			float floatValue = DatumGetFloat4(datum);
			DUCKDB_VECTOR_ASSIGN(data, float, offset, floatValue);
			break;
		}

		case FLOAT8OID:
		{
			double doubleValue = DatumGetFloat8(datum);
			DUCKDB_VECTOR_ASSIGN(data, double, offset, doubleValue);
			break;
		}

		case NUMERICOID:
		{
			double doubleValue =
				DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, datum));

			/* TODO: consider DECIMAL type */

			DUCKDB_VECTOR_ASSIGN(data, double, offset, doubleValue);
			break;
		}

		case INT2OID:
		{
			int16_t smallIntValue = DatumGetInt16(datum);
			DUCKDB_VECTOR_ASSIGN(data, int16_t, offset, smallIntValue);
			break;
		}

		case INT4OID:
		{
			int32_t intValue = DatumGetInt32(datum);
			DUCKDB_VECTOR_ASSIGN(data, int32_t, offset, intValue);
			break;
		}

		case INT8OID:
		{
			int64_t longValue = DatumGetInt64(datum);
			DUCKDB_VECTOR_ASSIGN(data, int64_t, offset, longValue);
			break;
		}

		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCOLLATIONOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
		{
			Oid pgOidValue = DatumGetObjectId(datum);
			uint32_t duckOidValue = (uint32_t) pgOidValue;
			DUCKDB_VECTOR_ASSIGN(data, uint32_t, offset, duckOidValue);
			break;
		}

		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		{
			text *textValue = (text *) DatumGetPointer(datum);
			char *string = VARDATA_ANY(textValue);
			int stringLength = VARSIZE_ANY_EXHDR(textValue);

			duckdb_vector_assign_string_element_len(vector, offset, string, stringLength);
			break;
		}

		case JSONBOID:
		{
			Jsonb *pgJsonbValue = DatumGetJsonbP(datum);

			StringInfoData jsonString;
			initStringInfo(&jsonString);

			(void) JsonbToCString(&jsonString, &pgJsonbValue->root,
								  VARSIZE(pgJsonbValue));

			duckdb_vector_assign_string_element_len(vector, offset, jsonString.data,
													jsonString.len);
			break;
		}

		case BYTEAOID:
		{
			bytea *pgByteArray = (bytea *) DatumGetPointer(datum);
			int byteArraySize = VARSIZE_ANY_EXHDR(pgByteArray);
			char *bytePointer = VARDATA_ANY(pgByteArray);

			/* TODO: is this ok for blob? */
			duckdb_vector_assign_string_element_len(vector, offset, bytePointer,
													byteArraySize);
			break;
		}

		case DATEOID:
		{
			DateADT pgDateValue = DatumGetDateADT(datum);

			/* Postgres starts at 2000-1-1, DuckDB starts at 1970-1-1 */
			duckdb_date duckDateValue = {
				pgDateValue + DAYS_BETWEEN_1970_2000
			};

			DUCKDB_VECTOR_ASSIGN(data, duckdb_date, offset, duckDateValue);
			break;
		}

		case TIMEOID:
		{
			TimeADT pgTimeValue = DatumGetTimeADT(datum);

			duckdb_time duckTimeValue = {
				pgTimeValue
			};

			DUCKDB_VECTOR_ASSIGN(data, duckdb_time, offset, duckTimeValue);
			break;
		}

		case TIMETZOID:
		{
			TimeTzADT *pgTimeTzValue = DatumGetTimeTzADTP(datum);

			duckdb_time duckTimeValue = {
				pgTimeTzValue->time + pgTimeTzValue->zone * MICROS_PER_SEC
			};

			DUCKDB_VECTOR_ASSIGN(data, duckdb_time, offset, duckTimeValue);
			break;
		}

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/* TODO: off by timezone */
			Timestamp pgTimestampValue = DatumGetTimestamp(datum);

			/* Postgres starts at 2000-1-1, DuckDB starts at 1970-1-1 */
			duckdb_timestamp duckTimestampValue = {
				pgTimestampValue + DAYS_BETWEEN_1970_2000 * MICROS_PER_DAY
			};

			DUCKDB_VECTOR_ASSIGN(data, duckdb_timestamp, offset, duckTimestampValue);
			break;
		}

		case INTERVALOID:
		{
			Interval *pgIntervalValue = DatumGetIntervalP(datum);

			duckdb_interval duckIntervalValue = {
				.micros = pgIntervalValue->time,
				.days = pgIntervalValue->day,
				.months = pgIntervalValue->month,
			};

			DUCKDB_VECTOR_ASSIGN(data, duckdb_interval, offset, duckIntervalValue);
			break;
		}

		case UUIDOID:
		{
			elog(ERROR, "cast from uuid not yet supported");
			break;
		}

		default:
		{
			elog(ERROR, "cast from type %d not yet supported", typeId);
			break;
		}
	}
}
