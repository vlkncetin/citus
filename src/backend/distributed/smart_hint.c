#include "postgres.h"
#include "distributed/smart_hint.h"

void
ReplaceCitusHintSmart(ErrorData *edata)
{
    edata->hint = pstrdup("Try to run the query with the smart hint");
}
