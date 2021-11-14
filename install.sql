create or replace procedure CREATE_MTI_STATEMENT(STAGE_PATH string, FILE_FORMAT string, TARGET_DATABASE string, TARGET_SCHEMA string, TARGET_TABLE string)
returns string
language javascript
execute as caller
as
$$
"use strict";

/********************************************************************************************************
*                                                                                                       *
*                            Snowflake Conditional MTI Statement Creator                                *
*                                                                                                       *
*  Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.                                     *
*                                                                                                       *
*  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in  *
*. compliance with the License. You may obtain a copy of the License at                                 *
*                                                                                                       *
*                               http://www.apache.org/licenses/LICENSE-2.0                              *
*                                                                                                       *
*  Unless required by applicable law or agreed to in writing, software distributed under the License    *
*  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  *
*  implied. See the License for the specific language governing permissions and limitations under the   *
*  License.                                                                                             *
*                                                                                                       *
*  Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.                                     *
*                                                                                                       *
********************************************************************************************************/


const RAW_COLUMN_SUFFIX    = "_raw";
const NONCONFORMING_SUFFIX = "_NONCONFORMING";

class Query{
    constructor(sql, binds){
        this.sql = sql;
        this.binds = binds;
    }
}

class Column {
    constructor(columnName, ordinalPosition, dataType, isNullable, characterMaxLength, numericPrecision, numericScale) {
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
        this.dataType = dataType;
        this.isNullable = isNullable;
        this.characterMaxLength = characterMaxLength;
        this.numericPrecision = numericPrecision;
        this.numericScale = numericScale;
        this.insert = `${column}`;
    }
}

/********************************************************************************************************
*  Main function.                                                                                       *
********************************************************************************************************/

let sql = getColumnSQL(TARGET_DATABASE);
let binds = [TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE];
let rs = getQuery(sql, binds).resultSet;
let row = {};
let inserts = "";
let c = 0;
let columnList = "";
let conditionList = "";
let rawColumnList = "";
let rawColumns = "";
let untypedColumnList = "";
while (rs.next()) {
    c++;
    if (inserts.length > 0) {
        inserts += "\n\t,";
        columnList += "\n\t,";
        rawColumnList += "\n\t,";
        rawColumns += "\n\t,";
        conditionList += "\n\tor ";
        untypedColumnList += "\n\t,";
    } else {
        inserts += "\t,";
        columnList += "\t ";
        rawColumnList += "\t ";
        rawColumns += "\t ";
        conditionList += "\t  ";
        untypedColumnList += "\t ";
    }
    row = getRow(rs);
    inserts += getInsert(row) + ` as "${row.columnName}"`;
    columnList += `"${row.columnName}"`;
    untypedColumnList += `"${row.columnName}" string`;
    rawColumnList += `"${row.columnName}${RAW_COLUMN_SUFFIX}"`;
    rawColumns += "$" + `${c} as "${row.columnName}${RAW_COLUMN_SUFFIX}"`;
    conditionList += `"${row.columnName}"\tis null and\t"${row.columnName}${RAW_COLUMN_SUFFIX}"\t is not null`;
}

return getMultiInsert(TARGET_TABLE, columnList, NONCONFORMING_SUFFIX, conditionList, rawColumnList, rawColumns, inserts, STAGE_PATH, FILE_FORMAT, untypedColumnList);

/********************************************************************************************************
*  End of Main function. Start of Data Type Dispatcher                                                  *
********************************************************************************************************/

function getRow(rs) {
    let row = {};
    row.columnName         = rs.getColumnValue("COLUMN_NAME");
    row.ordinalPosition    = rs.getColumnValue("ORDINAL_POSITION");
    row.dataType           = rs.getColumnValue("DATA_TYPE");
    row.isNullable         = rs.getColumnValue("IS_NULLABLE");
    row.characterMaxLength = rs.getColumnValue("CHARACTER_MAXIMUM_LENGTH");
    row.numericPrecision   = rs.getColumnValue("NUMERIC_PRECISION");
    row.numericScale       = rs.getColumnValue("NUMERIC_SCALE");
    return row;
}

function getInsert(row) {
    let col = "$" + row.ordinalPosition;
    switch (row.dataType) {
        case "ARRAY":
            return `iff(typeof(try_parse_json(${col})) = 'ARRAY', try_parse_json(${col}), null)`; 
        case "BINARY":
            return `try_to_binary("${col}")`;
        case "BOOLEAN":
            return `try_to_boolean("${col}")`;
        case "DATE":
            return `try_to_date("${col}")`;
        case "FLOAT":
            return `try_to_double("${col}")`;
        case "GEOGRAPHY":
            return `try_to_geography("${col}")`;
        case "NUMBER":
            return `try_to_number("${col}", ${row.numericPrecision}, ${row.numericScale})`;
        case "OBJECT":
            return `iff(typeof(try_parse_json(${col})) = 'OBJECT', try_parse_json(${col}), null)`;
        case "TEXT":
            return `iff(length("${col}") > ${row.characterMaxLength}, null, "${col}")`;
        case "TIMESTAMP_LTZ":
            return `try_to_timestamp_ltz(trim("${col}"))`;
        case "TIMESTAMP_NTZ":
            return `try_to_timestamp_ntz(trim("${col}"))`;
        case "TIMESTAMP_TZ":
            return `try_to_timestamp_tz(trim("${col}"))`;
        case "VARIANT":
            return `try_parse_json(trim("${col}"))`;
        default:
            throw "Unknown data type: " + row.dataType;
    }
}

/********************************************************************************************************
*  End of Class Dispatcher. Start of Helper Functions                                                   *
********************************************************************************************************/

function getMultiInsert(tableName, columnList, nonconformingSuffix, conditionList, rawColumnList, rawColumns, tryColumns, stageName, fileFormat) {
return `
insert first when
${conditionList}
then into ${tableName}${nonconformingSuffix}
(
${columnList}
)
values
(
${rawColumnList}
)
else into LINEITEM
(
${columnList}
)
values
(
${columnList}
)
select 
${rawColumns}

${tryColumns}

from ${stageName} (file_format => '${fileFormat}');
`;
}


function getQuery(sql, binds){
    let cmd = {sqlText: sql, binds: binds};
    let query = new Query(sql, binds);
    query.statement = snowflake.createStatement(cmd);
    query.resultSet = query.statement.execute();
    return query;
}

/********************************************************************************************************
*  SQL Template Functions                                                                               *
********************************************************************************************************/

function getColumnSQL(db) {
return `
select   COLUMN_NAME
        ,ORDINAL_POSITION
        ,IS_NULLABLE
        ,DATA_TYPE
        ,CHARACTER_MAXIMUM_LENGTH
        ,NUMERIC_PRECISION
        ,NUMERIC_SCALE
from "${db}".INFORMATION_SCHEMA.COLUMNS
where    TABLE_CATALOG=:1 
    and  TABLE_SCHEMA=:2
    and  TABLE_NAME=:3
order by ORDINAL_POSITION
`;
}

/********************************************************************************************************
*  MTI Template                                                                                         *
********************************************************************************************************/

function getMultiInsert(tableName, columnList, nonconformingSuffix, conditionList, rawColumnList, rawColumns, tryColumns, stageName, fileFormat, untypedColumnList) {
return `
create table "${tableName}${nonconformingSuffix}" (
${untypedColumnList}
);

---------------------

insert first when
${conditionList}
then into ${tableName}${nonconformingSuffix}
(
${columnList}
)
values
(
${rawColumnList}
)
else into LINEITEM
(
${columnList}
)
values
(
${columnList}
)
select 
${rawColumns}

${tryColumns}

from ${stageName} (file_format => '${fileFormat}');
`;
}

$$;
