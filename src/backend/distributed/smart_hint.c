#include "postgres.h"
#include "distributed/backend_data.h"
#include "distributed/smart_hint.h"
#include "distributed/coordinator_protocol.h"
#include "lib/stringinfo.h"
#include "tcop/tcopprot.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "miscadmin.h"


bool EnableHintAI = false;


static void SaveClusterStatQueryViaSPI(FILE *fp);


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


void
ReplaceCitusHintSmart(ErrorData *edata)
{
    if (!EnableHintAI)
    {
        return;
    }

    if (edata->elevel != ERROR)
    {
        return;
    }

    if (IsBackgroundWorker ||
        !IsCoordinator() ||
        IsCitusInternalBackend() ||
        IsRebalancerInternalBackend())
    {
        return;
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

    SaveClusterStatQueryViaSPI(fp);

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

}
