#include "postgres.h"
#include "distributed/backend_data.h"
#include "distributed/smart_hint.h"
#include "distributed/coordinator_protocol.h"
#include "lib/stringinfo.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "miscadmin.h"


bool EnableHintAI = false;


static void SaveClusterStatQueryViaSPI(FILE *fp);
static void RunHintAI(void);


static void
SaveClusterStatQueryViaSPI(FILE *fp)
{
    const char *cmd = "SELECT pg_catalog.get_cluster_stats()";

	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	spiResult = SPI_execute(cmd, true, 1);
	if (spiResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

    bool isnull = false;
    Datum resultDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

    Form_pg_attribute attr = TupleDescAttr(SPI_tuptable->tupdesc, 0);
    resultDatum = SPI_datumTransfer(resultDatum, attr->attbyval, attr->attlen);

    char *result = TextDatumGetCString(resultDatum);
    fprintf(fp, "%s\n", result);

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}
}


static void
RunHintAI(void)
{
    elog(NOTICE, "Running the HintAI code...");
    system("psql -p 9700 -X -f /tmp/hint_ai_code.txt --single-transaction 2> /tmp/hint_ai_code_error.txt 1> /tmp/hint_ai_code_output.txt");

    // read the first ERROR-DETAIL-HINT block from the error file
    FILE *fp = fopen("/tmp/hint_ai_code_error.txt", "r");
    if (fp == NULL)
    {
        elog(ERROR, "Failed to open file");
    }

    char *error = NULL;
    char *detail = NULL;
    char *hint = NULL;
    while (true)
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read = getline(&line, &len, fp);
        if (read == -1)
        {
            break;
        }

        char *got_error = strstr(line, "ERROR:  ");
        if (got_error && error)
        {
            // we have already read the error, so we are done
            break;
        }

        if (!error && got_error)
        {
            error = got_error + strlen("ERROR:  ");
            error[strlen(error) - 1] = '\0'; // remove the trailing '\n'
        }

        char *got_detail = strstr(line, "DETAIL:  ");
        if (!detail && got_detail)
        {
            detail = got_detail + strlen("DETAIL:  ");
            detail[strlen(detail) - 1] = '\0'; // remove the trailing '\n'
        }

        char *got_hint = strstr(line, "HINT:  ");
        if (!hint && got_hint)
        {
            hint = got_hint + strlen("HINT:  ");
            hint[strlen(hint) - 1] = '\0'; // remove the trailing '\n'
        }
    }

    fclose(fp);

    if (error)
    {
        ereport(ERROR, (errmsg("%s", error),
                          detail ? errdetail("%s", detail) : 0,
                          hint ? errhint("%s", hint) : 0));
    }

    // else, print lines from /tmp/hint_ai_code_output.txt
    fp = fopen("/tmp/hint_ai_code_output.txt", "r");
    if (fp == NULL)
    {
        elog(ERROR, "Failed to open file");
    }

    StringInfo outBuf = makeStringInfo();

    while (true)
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read = getline(&line, &len, fp);
        if (read == -1)
        {
            break;
        }

        appendStringInfoString(outBuf, line);
    }

    if (outBuf->len > 0)
    {
        ereport(NOTICE, (errmsg("The output of \"RUN HINT\":\n%s", outBuf->data)));
    }

    fclose(fp);
}


// return true if should skip prior hooks
bool
ReplaceCitusHintSmart(ErrorData *edata)
{
    if (!EnableHintAI)
    {
        return false;
    }

    if (edata->elevel != ERROR)
    {
        return false;
    }

    if (IsBackgroundWorker ||
        !IsCoordinator() ||
        IsCitusInternalBackend() ||
        IsRebalancerInternalBackend())
    {
        return false;
    }

	set_config_option("citus.enable_hint_ai", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

    // run the query saved in /tmp/hint_ai_code.txt if the command is "RUN;", using system
    if (strcasecmp(debug_query_string, "RUN HINT;") == 0)
    {
        RunHintAI();

        return true;
    }

    ereport(NOTICE, (errmsg("HintAI is enabled, waiting for ChatGPT to "
                            "report the error with a smart hint...")));

    FILE *fp = fopen("/tmp/hint_ai_prompt.txt", "w");
    if (fp == NULL)
    {
        elog(ERROR, "Failed to open file");
    }

    if (debug_query_string)
    {
        fprintf(fp, "%s\n", debug_query_string);
    }

    if (edata->message)
    {
        fprintf(fp, "ERROR:  %s\n", edata->message);
    }

    if (edata->detail)
    {
        fprintf(fp, "DETAIL:  %s\n", edata->detail);
    }

    fprintf(fp, "------\n");

    if (ActivePortal && ActiveSnapshotSet())
    {
        SaveClusterStatQueryViaSPI(fp);
    }

    fclose(fp);

    // run the command
	const char *command = "python3.9 /home/onurctirtir/hackathon/hint_ai_cli.py";
    int ret = system(command);
    if (ret != 0)
    {
        elog(ERROR, "Failed to run command");
    }

    // read the result
    fp = fopen("/tmp/hint_ai_response.txt", "r");
    if (fp == NULL)
    {
        elog(ERROR, "Failed to open file");
    }

    // read the lines into edata->hint
    StringInfoData buf;
    initStringInfo(&buf);
    while (true)
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read = getline(&line, &len, fp);
        if (read == -1)
        {
            break;
        }

        appendStringInfoString(&buf, line);
    }

    edata->hint = buf.data;

    fclose(fp);

    return false;
}
