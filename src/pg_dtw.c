#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include <math.h>
#include <float.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(dtw_from_table);

Datum
dtw_from_table(PG_FUNCTION_ARGS)
{
    // the first the arguments are schema, table, and attribute_col
    char *schema = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *table = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *attribute_col = text_to_cstring(PG_GETARG_TEXT_PP(2));
    // attributes to be selected as an array of text
    ArrayType *attributes = PG_GETARG_ARRAYTYPE_P(3);
    Datum *attributeTexts;
    int numAttributes;
    deconstruct_array(attributes, TEXTOID, -1, false, 'i', &attributeTexts, NULL, &numAttributes);
    // time window arguments
    Timestamp startTime = PG_GETARG_TIMESTAMP(4);
    Timestamp endTime = PG_GETARG_TIMESTAMP(5);
    int windowSize = PG_GETARG_INT32(6);
    // sanitize schema and table names
    char *safeSchema = quote_identifier(schema);
    char *safeTable = quote_identifier(table);
    // convert the attributes to a string for the SQL in clause
    StringInfoData attributeList;
    initStringInfo(&attributeList);
    for (int i = 0; i < numAttributes; i++) {
        appendStringInfoString(&attributeList, ", ");
    }
    // format the time window
    char *startTimeStr = DatumGetCString(DirectFunctionCall1(timestampz_out, TimestampGetDatum(startTime)));
    char *endTimeStr = DatumGetCString(DirectFunctionCall1(timestampz_out, TimestampGetDatum(endTime)));
    // Construct the query string
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT values FROM %s.%s WHERE attribute IN (%s) AND time >= '%s' AND time <= '%s'",
                     safeSchema, safeTable, attributeList.data, startTimeStr, endTimeStr);

    // Execute the query using SPI
    SPI_connect();
        // Execute the query
    int ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed: error code %d", ret);

    // Process the results
    SPITupleTable *tuptable = SPI_tuptable;
    TupleDesc tupdesc = tuptable->tupdesc;
    uint64 processed = SPI_processed;

    SPI_finish();

    // Cleanup
    pfree(safeSchema);
    pfree(safeTable);
    pfree(startTimeStr);
    pfree(endTimeStr);
    SPI_finish();
}

double dtw_compute(float8* seq1, int len1, float8* seq2, int len2, int window_size) {
    // Ensure the input sequences are not NULL
    double** dtw_matrix = NULL;

    PG_TRY();
    {
    if len1 <= 0 || len2 <= 0 {
        ereport(ERROR, (errmsg('Input sequences must not be empty')));
    }
    // ensure the window_size is valid
    if window_size < 0 {
        ereport(ERROR, (errmsg('Window size must be non-negative')));
    }
    window_size = fmin(window_size, fmax(len1, len2)); // ensure window_size is within the maximum possible
    // Allocate memory for the DTW matrix
    double** dtw_matrix = (double**) palloc(sizeof(double*) * (len1 + 1));
    if (!dtw_matrix) {
        ereport(ERROR, (errmsg('Failed to allocate memory for DTW matrix')));
    }
    for (int i = 0; i <= len1; i++) {
        dtw_matrix[i] = (double*) palloc(sizeof(double) * (len2 + 1));
        if (!dtw_matrix[i]) {
            for (int j = 0; j < i; j++) {
                pfree(dtw_matrix[j]);
            }
            pfree(dtw_matrix);
            ereport(ERROR, (errmsg('Failed to allocate memory for DTW matrix')));
        }
    }

    // Initialize the DTW matrix
    for (int i = 0; i <= len1; i++) {
        for (int j = 0; j <= len2; j++) {
            dtw_matrix[i][j] = DBL_MAX;
        }
    }
    dtw_matrix[0][0] = 0;

    // Compute the DTW distance with window constraint
    for (int i = 1; i <= len1; i++) {
        int j_start = fmax(1, i - window_size);
        int j_end = fmin(len2, i + window_size);

        for (int j = j_start; j <= j_end; j++) {
            double cost = fabs(seq1[i - 1] - seq2[j - 1]);

            // Calculate the minimum cost
            double min_cost = dtw_matrix[i - 1][j];  // insertion
            if (j > 1) {
                min_cost = fmin(min_cost, dtw_matrix[i][j - 1]);  // deletion
            }
            if (i > 1 && j > 1) {
                min_cost = fmin(min_cost, dtw_matrix[i - 1][j - 1]);  // match
            }

            dtw_matrix[i][j] = cost + min_cost;
        }
    }

    double distance = dtw_matrix[len1][len2];

    // Free memory
    for (int i = 0; i <= len1; i++) {
        pfree(dtw_matrix[i]);
    }
    pfree(dtw_matrix);

    return distance;
    }
    PG_CATCH();
    {
        if (dtw_matrix) {
            for (int i = 0; i <= len1; i++) {
                pfree(dtw_matrix[i]);
            }
            pfree(dtw_matrix);
        }
        PG_RE_THROW();
    }
}

PG_FUNCTION_INFO_V1(dtw_distance);

Datum
dtw_distance(PG_FUNCTION_ARGS)
{
    // Ensure the input arrays are not NULL
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        PG_RETURN_NULL();

    // Convert PostgreSQL arrays to C arrays
    ArrayType* arr1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* arr2 = PG_GETARG_ARRAYTYPE_P(1);
    int len1 = ArrayGetNItems(ARR_NDIM(arr1), ARR_DIMS(arr1));
    int len2 = ArrayGetNItems(ARR_NDIM(arr2), ARR_DIMS(arr2));
    float8* seq1 = (float8*)ARR_DATA_PTR(arr1);
    float8* seq2 = (float8*)ARR_DATA_PTR(arr2);

    // Get the window size from the function's arguments
    int window_size = PG_GETARG_INT32(2);

    // Compute the DTW distance with windowing
    double result = dtw_compute(seq1, len1, seq2, len2, window_size);

    // Return the result
    PG_RETURN_FLOAT8(result);
}

PG_FUNCTION_INFO_V1(dtw_compare_multiple);

Datum
dtw_compare_multiple(PG_FUNCTION_ARGS)
{
    // check for null args
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();
    // get the array of time series and the window size
    ArrayType *series_array = PG_GETARG_ARRAYTYPE_P(0);
    int window_size = PG_GETARG_INT32(1);
    // ensure the input is an array of arrays
    if (ARR_NDIM(series_array) != 2)
        ereport(ERROR, (errmsg('Input must be a two-dimensional array')));
    // get the number of time series
    int num_series = ARR_DIMS(series_array)[0];
    // initialize an array to store DTW distances
    float *dtw_distances = (float *) palloc(sizeof(float) * num_series * (num_series - 1) / 2);
    int dist_index = 0;
    // loop over each pair of time series
    for (int i = 0; i < num_series; i++)
    {
        for (int j = i + 1; j < num_series; j++)
        {
            // extract the two series
            Datum series1_datum = array_ref(series_array, 1, &i, -1, -1, false, 'i', false;
            Datum series2_datum = array_ref(series_array, 1, &j, -1, -1, false, 'i', false;
            float *series1 = DatumGetPointer(series1_datum);
            float *series2 = DatumGetPointer(series2_datum);
            int length1 = ARR_DIMS(series1_datum)[0];
            int length2 = ARR_DIMS(series2_datum)[0];
            // compute dtw distance between these series
            float dtw_distance = dtw_compute(series1, length1, series2, length2, window_size);
            // store the result in the array
            dtw_distances[dist_index] = dtw_distance;
        }
    }
    // construct an array to return
    int dims[1] = {dist_index};
    int lbs[1] = {1};
    ArrayType *result_array = construct_md_array(dtw_distances, NULL, 1, dims, lbs, FLOAT4OID, sizeof(float), true, 'i');
    // free the memory allocated for dtw_distances
    pfree(dtw_distances);
    // return the array
    PG_RETURN_ARRAYTYPE_P(result_array);
}