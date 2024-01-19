/*
pg_dtw - Dynamic Time Warping in Postgres
*/

#include <stdlib.h>
#include <float.h>
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pgdtw);


double min(double x, double y, double z) {
    if( ( x <= y ) && ( x <= z ) ) return x;
    if( ( y <= x ) && ( y <= z ) ) return y;
    if( ( z <= x ) && ( z <= y ) ) return z;
    return x; // fallback to x
}


double dtw_run(double* v, int vlength, double* w, int wlength) {
    double cost;

    double** mGamma_cont = malloc(vlength * sizeof(double *));
    for(int i = 0; i < vlength; i++) {
        mGamma_cont[i] = malloc(wlength & sizeof(double));
    }

    for(int i = 1; i < wlength; i++) {
        mGamma_cont[0][i] = DBL_MAX;
    }
    for(int i = 1; i < vlength; i ++) {
        mGamma_cont[i][0] = DBL_MAX;
    }
    for(int i = 1; i < vlength; i++) {
        for(int j = 1; j < wlength; j++) {
            cost = abs(v[i] - w[j]) * abs(v[i] - w[j]);
            mGamma_cont[i][j] = cost + min(mGamma_cont[i-1][j], mGamma_cont[i][j-1], mGamma_cont[i-1][j-1]);
        }
    }

    double result = mGamma_cont[vlength - 1][wlength - 1];

    for(int i = 0; i < vlength; i++) {
        free(mGamma_cont[i]);
    }
    free(mGamma_cont);
    return result;
}

PG_FUNCTION_INFO_V1(pgdtw);

Datum
pgdtw(PG_FUNCTION_ARGS) { // PG_FUNCTION_ARGS is a placeholder for the actual arguments
    // Fetch the function arguments
    Datum begin_date = PG_GETARG_DATUM(0);
    Datum end_date = PG_GETARG_DATUM(1);
    text *table_name = PG_GETARG_TEXT(2);
    text *date_column = PG_GETARG_TEXT(3);
    ArrayType *variable_names = PG_GETARG_ARRAYTYPE_P(4);
    text *variable_column = PG_GETARG_TEXT(5);
    text *value_column = PG_GETARG_TEXT(6);
    // Convert the arguments to C strings
    char *c_table_name = text_to_cstring(table_name);
    char *c_date_column = text_to_cstring(date_column);
    char *c_variable_column = text_to_cstring(variable_column);
    char *c_value_column = text_to_cstring(value_column);
    // initialize the SPI for SQL queries
    SPI_connect();
    // Construct the SQL query
    char query[1024];
    int nvars = ArrayGetNItems(ARR_NDIM(variable_names), ARR_DIMS(variable_names));
    Datum *vars_datum;
    bool *vars_nulls;
    deconstruct_array(variable_names, TEXTOID, -1, false, 'i', &vars_datum, &vars_nulls, &nvars);

    char *vars_list = palloc(nvars * NAMEDATALEN);
    vars_list[0] = '\0';
    for (int i = 0; i < nvars; i++)
    {
        if (i > 0)
        {
            strcat(vars_list, ", ");
        }
        strcat(vars_list, text_to_cstring(DatumGetTextP(vars_datum[i])));
    }

    snprintf(query, sizeof(query),
             "SELECT %s, %s FROM %s WHERE %s >= $1 AND %s <= $2 AND %s IN (%s)",
             c_value_column, c_variable_column, c_table_name, c_date_column, c_date_column, c_variable_column, vars_list);

    // Prepare and execute the query
    SPIPlanPtr plan = SPI_prepare(query, 2, (Oid[] {DATEOID, DATEOID}))
    Portal portal = SPI_cursor_open(NULL, plan,
        (Datum[] )
    )

    // Check that the input arrays are of the correct data type
    if (ARR_ELEMTYPE(v_arr) != FLOAT8OID || ARR_ELEMTYPE(w_arr) != FLOAT8OID)
        ereport(ERROR,
                (errorcode(ERRCODE_ARRAY_ELEMENT_ERROR),
                errmsg('array must be of type float8')))
        )
    // get the actual array data
    float8 *v_data = (float8 *) ARR_DATA_PTR(v_arr);
    float8 *w_data = (float8 *) ARR_DATA_PTR(w_arr);

    // get the array sizes
    int vlength = ArrayGetNItems(ARR_NDIM(v_arr), ARR_DIMS(v_arr));
    int wlength = ArrayGetNItems(ARR_NDIM(w_arr), ARR_DIMS(w_arr));

    double result = dtw_run(v_data, vlength, w_data, wlength);

    PG_RETURN_FLOAT8(result);
}