CREATE OR REPLACE FUNCTION 
    pgdtw(
        begin_date date,       -- the beginning date for DTW analysis
        end_date date,         -- the end date for DTW analysis
        table_name text,       -- the name of the table to analyze
        date_column text,      -- the name of the date column
        variable_names text[], -- the variable names in the variable column
        variable_column text,  -- the name of the variable column
        value_column text      -- the name of the value column
    ) 
RETURNS float8
AS 'MODULE_PATHNAME', 'pgdtw' 
LANGUAGE C STRICT;
