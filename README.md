<a href="https://community-extensions.duckdb.org/extensions/chsql.html" target="_blank">
<img src="https://github.com/user-attachments/assets/9003897d-db6f-4a79-9443-9b72766b511b" width=200>
</a>

# DuckDB ClickHouse SQL extension

The DuckDB [chsql](https://community-extensions.duckdb.org/extensions/chsql.html) community extension implements popular **ClickHouse SQL** syntax macros and functions,<br>
making it easier for users to transition between the two database systems ⭐ designed for [Quackpipe](https://github.com/metrico/quackpipe) 

<br>

## Installation

**chsql** is distributed as a [DuckDB Community Extension](https://github.com/duckdb/community-extensions) and can be installed using SQL:

```sql
INSTALL chsql FROM community;
LOAD chsql;
```

If you previously installed the `chsql` extension, upgrade using the FORCE command
```sql
FORCE INSTALL chsql FROM community;
LOAD chsql;
```

## Usage Examples
Once installed, the [macro functions](https://community-extensions.duckdb.org/extensions/chsql.html#added-functions) provided by the extension can be used just like built-in functions.

Here's a random example out of 100s using the `IPv4StringToNum` and `IPv4NumToString` functions:

```sql
D INSTALL chsql FROM community;
D LOAD chsql;
D SELECT IPv4StringToNum('127.0.0.1'), IPv4NumToString(2130706433);
┌──────────────────────────────┬─────────────────────────────┐
│ ipv4stringtonum('127.0.0.1') │ ipv4numtostring(2130706433) │
│            int32             │           varchar           │
├──────────────────────────────┼─────────────────────────────┤
│                   2130706433 │ 127.0.0.1                   │
└──────────────────────────────┴─────────────────────────────┘
```

### Remote Queries
The built-in `ch_scan` function can be used to query remote ClickHouse servers using the HTTP/s API

```sql
D SELECT * FROM ch_scan("SELECT number * 2 FROM numbers(10)", "https://play.clickhouse.com");
```

## Supported Functions

👉 The [list of supported aliases](https://community-extensions.duckdb.org/extensions/chsql.html#added-functions) is available on the [dedicated extension page](https://community-extensions.duckdb.org/extensions/chsql.html)<br>
👉 The combined list of [supported functions](https://quackpipe.fly.dev/?user=default#TE9BRCBjaHNxbDsgU0VMRUNUIERJU1RJTkNUIE9OKGZ1bmN0aW9uX25hbWUpIGZ1bmN0aW9uX25hbWUgYXMgbmFtZQpGUk9NIGR1Y2tkYl9mdW5jdGlvbnMoKSBXSEVSRSBuYW1lIElOIChTRUxFQ1QgbmFtZSBGUk9NIGNoX3NjYW4oJyBTRUxFQ1QgbmFtZSBGUk9NIHN5c3RlbS5mdW5jdGlvbnMnLCdodHRwczovL3BsYXkuY2xpY2tob3VzZS5jb20nKSBPUkRFUiBCWSBuYW1lKSBPUkRFUiBCWSBuYW1lOw==) can be obtained using an [SQL Join](https://quackpipe.fly.dev/?user=default#TE9BRCBjaHNxbDsgU0VMRUNUIERJU1RJTkNUIE9OKGZ1bmN0aW9uX25hbWUpIGZ1bmN0aW9uX25hbWUgYXMgbmFtZQpGUk9NIGR1Y2tkYl9mdW5jdGlvbnMoKSBXSEVSRSBuYW1lIElOIChTRUxFQ1QgbmFtZSBGUk9NIGNoX3NjYW4oJyBTRUxFQ1QgbmFtZSBGUk9NIHN5c3RlbS5mdW5jdGlvbnMnLCdodHRwczovL3BsYXkuY2xpY2tob3VzZS5jb20nKSBPUkRFUiBCWSBuYW1lKSBPUkRFUiBCWSBuYW1lOw==)

<br>


## Motivation

> Why is the DuckDB + chsql combo fun and useful

✔ DuckDB SQL is awesome and full of great functions.<br>
✔ ClickHouse SQL is awesome and full of great functions. 

✔ The DuckDB library is ~51M and modular. Can LOAD extensions.<br>
❌ The ClickHouse monolith is ~551M and growing. No extensions. 

✔ DuckDB is open source and _protected by a no-profit foundation._<br>
❌ ClickHouse is open core and _controlled by for-profit corporation._ 

✔ DuckDB embedded is fast, mature and elegantly integrated in many languages.<br>
❌ chdb is still experimental, unstable and _currently only supports Python_. 

<img src="https://github.com/user-attachments/assets/a17efd68-d2e1-42a7-8ab9-1ea4c2ff11e3" width=700 />

<br>

<br>


## Functions
| function               | fun_type    | description                                                                                  | comment                                       | example                                                                                              |
| ---------------------- | ----------- | -------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| IPv4NumToString        | macro       | Cast IPv4 address from numeric to string format                                              |                                               | SELECT IPv4NumToString(2130706433);                                                                  |
| IPv4StringToNum        | macro       | Cast IPv4 address from string to numeric format                                              |                                               | SELECT IPv4StringToNum('127.0.0.1');                                                                 |
| arrayExists            | macro       | Check if any element of the array satisfies the condition                                    |                                               | SELECT arrayExists(x -> x = 1, [1, 2, 3]);                                                           |
| arrayJoin              | macro       | Unroll an array into multiple rows                                                           |                                               | SELECT arrayJoin([1, 2, 3]);                                                                         |
| arrayMap               | macro       | Applies a function to each element of an array                                               |                                               | SELECT arrayMap(x -> x + 1, [1, 2, 3]);                                                              |
| bitCount               | macro       | Counts the number of set bits in an integer                                                  |                                               | SELECT bitCount(15);                                                                                 |
| ch_scan                | table_macro | Query a remote ClickHouse server using HTTP/s API                                            | Returns the query results                     | SELECT * FROM ch_scan('SELECT version()','https://play.clickhouse.com', format := 'parquet');        |
| domain                 | macro       | Extracts the domain from a URL                                                               |                                               | SELECT domain('https://clickhouse.com/docs');                                                        |
| empty                  | macro       | Check if a string is empty                                                                   |                                               | SELECT empty('');                                                                                    |
| extractAllGroups       | macro       | Extracts all matching groups from a string using a regular expression                        |                                               | SELECT extractAllGroups('(\\d+)', 'abc123');                                                         |
| formatDateTime         | macro       | Formats a DateTime value into a string                                                       |                                               | SELECT formatDateTime(now(), '%Y-%m-%d');                                                            |
| generateUUIDv4         | macro       | Generate a UUID v4 value                                                                     |                                               | SELECT generateUUIDv4();                                                                             |
| ifNull                 | macro       | Returns the first argument if not NULL, otherwise the second                                 |                                               | SELECT ifNull(NULL, 'default');                                                                      |
| intDiv                 | macro       | Performs integer division                                                                    |                                               | SELECT intDiv(10, 3);                                                                                |
| intDivOZero            | macro       | Performs integer division but returns zero instead of throwing an error for division by zero |                                               | SELECT intDivOZero(10, 0);                                                                           |
| intDivOrNull           | macro       | Performs integer division but returns NULL instead of throwing an error for division by zero |                                               | SELECT intDivOrNull(10, 0);                                                                          |
| leftPad                | macro       | Pads a string on the left to a specified length                                              |                                               | SELECT leftPad('abc', 5, '*');                                                                       |
| lengthUTF8             | macro       | Returns the length of a string in UTF-8 characters                                           |                                               | SELECT lengthUTF8('Привет');                                                                         |
| match                  | macro       | Performs a regular expression match on a string                                              |                                               | SELECT match('abc123', '\\d+');                                                                      |
| minus                  | macro       | Performs subtraction of two numbers                                                          |                                               | SELECT minus(5, 3);                                                                                  |
| modulo                 | macro       | Calculates the remainder of division (modulus)                                               |                                               | SELECT modulo(10, 3);                                                                                |
| moduloOrZero           | macro       | Calculates modulus but returns zero instead of error on division by zero                     |                                               | SELECT moduloOrZero(10, 0);                                                                          |
| notEmpty               | macro       | Check if a string is not empty                                                               |                                               | SELECT notEmpty('abc');                                                                              |
| numbers                | table_macro | Generates a sequence of numbers starting from 0                                              | Returns a table with a single column (UInt64) | SELECT * FROM numbers(10);                                                                           |
| parseURL               | macro       | Extracts parts of a URL                                                                      |                                               | SELECT parseURL('https://clickhouse.com', 'host');                                                   |
| path                   | macro       | Extracts the path from a URL                                                                 |                                               | SELECT path('https://clickhouse.com/docs');                                                          |
| plus                   | macro       | Performs addition of two numbers                                                             |                                               | SELECT plus(5, 3);                                                                                   |
| protocol               | macro       | Extracts the protocol from a URL                                                             |                                               | SELECT protocol('https://clickhouse.com');                                                           |
| read_parquet_mergetree | function    | Merge parquet files using a primary sorting key for fast range queries                       | experimental                                  | COPY (SELECT * FROM read_parquet_mergetree(['/folder/*.parquet'], 'sortkey') TO 'sorted.parquet';    |
| rightPad               | macro       | Pads a string on the right to a specified length                                             |                                               | SELECT rightPad('abc', 5, '*');                                                                      |
| splitByChar            | macro       | Splits a string by a given character                                                         |                                               | SELECT splitByChar(',', 'a,b,c');                                                                    |
| toDayOfMonth           | macro       | Extracts the day of the month from a date                                                    |                                               | SELECT toDayOfMonth('2023-09-10');                                                                   |
| toFixedString          | macro       | Converts a value to a fixed-length string                                                    |                                               | SELECT toFixedString('abc', 5);                                                                      |
| toFloat                | macro       | Converts a value to a float                                                                  |                                               | SELECT toFloat('123.45');                                                                            |
| toFloatOrNull          | macro       | Converts a value to float or returns NULL if the conversion fails                            |                                               | SELECT toFloatOrNull('abc');                                                                         |
| toFloatOrZero          | macro       | Converts a value to float or returns zero if the conversion fails                            |                                               | SELECT toFloatOrZero('abc');                                                                         |
| toHour                 | macro       | Extracts the hour from a DateTime value                                                      |                                               | SELECT toHour(now());                                                                                |
| toInt128               | macro       | Converts a value to a 128-bit integer                                                        |                                               | SELECT toInt128('123456789012345678901234567890');                                                   |
| toInt128OrNull         | macro       | Converts to a 128-bit integer or returns NULL on failure                                     |                                               | SELECT toInt128OrNull('abc');                                                                        |
| toInt128OrZero         | macro       | Converts to a 128-bit integer or returns zero on failure                                     |                                               | SELECT toInt128OrZero('abc');                                                                        |
| toInt16                | macro       | Converts a value to a 16-bit integer                                                         |                                               | SELECT toInt16('123');                                                                               |
| toInt16OrNull          | macro       | Converts to a 16-bit integer or returns NULL on failure                                      |                                               | SELECT toInt16OrNull('abc');                                                                         |
| toInt16OrZero          | macro       | Converts to a 16-bit integer or returns zero on failure                                      |                                               | SELECT toInt16OrZero('abc');                                                                         |
| toInt256               | macro       | Converts a value to a 256-bit integer                                                        |                                               | SELECT toInt256('12345678901234567890123456789012345678901234567890123456789012345678901234567890'); |
| toInt256OrNull         | macro       | Converts to a 256-bit integer or returns NULL on failure                                     |                                               | SELECT toInt256OrNull('abc');                                                                        |
| toInt256OrZero         | macro       | Converts to a 256-bit integer or returns zero on failure                                     |                                               | SELECT toInt256OrZero('abc');                                                                        |
| toInt32                | macro       | Converts a value to a 32-bit integer                                                         |                                               | SELECT toInt32('123');                                                                               |
| toInt32OrNull          | macro       | Converts to a 32-bit integer or returns NULL on failure                                      |                                               | SELECT toInt32OrNull('abc');                                                                         |
| toInt32OrZero          | macro       | Converts to a 32-bit integer or returns zero on failure                                      |                                               | SELECT toInt32OrZero('abc');                                                                         |
| toInt64                | macro       | Converts a value to a 64-bit integer                                                         |                                               | SELECT toInt64('123');                                                                               |
| toInt64OrNull          | macro       | Converts to a 64-bit integer or returns NULL on failure                                      |                                               | SELECT toInt64OrNull('abc');                                                                         |
| toInt64OrZero          | macro       | Converts to a 64-bit integer or returns zero on failure                                      |                                               | SELECT toInt64OrZero('abc');                                                                         |
| toInt8                 | macro       | Converts a value to an 8-bit integer                                                         |                                               | SELECT toInt8('123');                                                                                |
| toInt8OrNull           | macro       | Converts to an 8-bit integer or returns NULL on failure                                      |                                               | SELECT toInt8OrNull('abc');                                                                          |
| toInt8OrZero           | macro       | Converts to an 8-bit integer or returns zero on failure                                      |                                               | SELECT toInt8OrZero('abc');                                                                          |
| toMinute               | macro       | Extracts the minute from a DateTime value                                                    |                                               | SELECT toMinute(now());                                                                              |
| toMonth                | macro       | Extracts the month from a Date value                                                         |                                               | SELECT toMonth('2023-09-10');                                                                        |
| toSecond               | macro       | Extracts the second from a DateTime value                                                    |                                               | SELECT toSecond(now());                                                                              |
| toString               | macro       | Converts a value to a string                                                                 |                                               | SELECT toString(123);                                                                                |
| toUInt16               | macro       | Converts a value to an unsigned 16-bit integer                                               |                                               | SELECT toUInt16('123');                                                                              |
| toUInt16OrNull         | macro       | Converts to an unsigned 16-bit integer or returns NULL on failure                            |                                               | SELECT toUInt16OrNull('abc');                                                                        |
| toUInt16OrZero         | macro       | Converts to an unsigned 16-bit integer or returns zero on failure                            |                                               | SELECT toUInt16OrZero('abc');                                                                        |
| toUInt32               | macro       | Converts a value to an unsigned 32-bit integer                                               |                                               | SELECT toUInt32('123');                                                                              |
| toUInt32OrNull         | macro       | Converts to an unsigned 32-bit integer or returns NULL on failure                            |                                               | SELECT toUInt32OrNull('abc');                                                                        |
| toUInt32OrZero         | macro       | Converts to an unsigned 32-bit integer or returns zero on failure                            |                                               | SELECT toUInt32OrZero('abc');                                                                        |
| toUInt64               | macro       | Converts a value to an unsigned 64-bit integer                                               |                                               | SELECT toUInt64('123');                                                                              |
| toUInt64OrNull         | macro       | Converts to an unsigned 64-bit integer or returns NULL on failure                            |                                               | SELECT toUInt64OrNull('abc');                                                                        |
| toUInt64OrZero         | macro       | Converts to an unsigned 64-bit integer or returns zero on failure                            |                                               | SELECT toUInt64OrZero('abc');                                                                        |
| toUInt8                | macro       | Converts a value to an unsigned 8-bit integer                                                |                                               | SELECT toUInt8('123');                                                                               |
| toUInt8OrNull          | macro       | Converts to an unsigned 8-bit integer or returns NULL on failure                             |                                               | SELECT toUInt8OrNull('abc');                                                                         |
| toUInt8OrZero          | macro       | Converts to an unsigned 8-bit integer or returns zero on failure                             |                                               | SELECT toUInt8OrZero('abc');                                                                         |
| toYYYYMM               | macro       | Formats a Date to 'YYYYMM' string format                                                     |                                               | SELECT toYYYYMM('2023-09-10');                                                                       |
| toYYYYMMDD             | macro       | Formats a Date to 'YYYYMMDD' string format                                                   |                                               | SELECT toYYYYMMDD('2023-09-10');                                                                     |
| toYYYYMMDDhhmmss       | macro       | Formats a DateTime to 'YYYYMMDDhhmmss' string format                                         |                                               | SELECT toYYYYMMDDhhmmss(now());                                                                      |
| toYear                 | macro       | Extracts the year from a Date or DateTime value                                              |                                               | SELECT toYear('2023-09-10');                                                                         |
| topLevelDomain         | macro       | Extracts the top-level domain (TLD) from a URL                                               |                                               | SELECT topLevelDomain('https://example.com');                                                        |
| tupleConcat            | macro       | Concatenates two tuples into one tuple                                                       |                                               | SELECT tupleConcat((1, 'a'), (2, 'b'));                                                              |
| tupleDivide            | macro       | Performs element-wise division between two tuples                                            |                                               | SELECT tupleDivide((10, 20), (2, 5));                                                                |
| tupleDivideByNumber    | macro       | Divides each element of a tuple by a number                                                  |                                               | SELECT tupleDivideByNumber((10, 20), 2);                                                             |
| tupleIntDiv            | macro       | Performs element-wise integer division between two tuples                                    |                                               | SELECT tupleIntDiv((10, 20), (3, 4));                                                                |
| tupleIntDivByNumber    | macro       | Performs integer division of each element of a tuple by a number                             |                                               | SELECT tupleIntDivByNumber((10, 20), 3);                                                             |
| tupleMinus             | macro       | Performs element-wise subtraction between two tuples                                         |                                               | SELECT tupleMinus((10, 20), (5, 3));                                                                 |
| tupleModulo            | macro       | Performs element-wise modulus between two tuples                                             |                                               | SELECT tupleModulo((10, 20), (3, 6));                                                                |
| tupleModuloByNumber    | macro       | Calculates the modulus of each element of a tuple by a number                                |                                               | SELECT tupleModuloByNumber((10, 20), 3);                                                             |
| tupleMultiply          | macro       | Performs element-wise multiplication between two tuples                                      |                                               | SELECT tupleMultiply((10, 20), (2, 5));                                                              |
| tupleMultiplyByNumber  | macro       | Multiplies each element of a tuple by a number                                               |                                               | SELECT tupleMultiplyByNumber((10, 20), 3);                                                           |
| tuplePlus              | macro       | Performs element-wise addition between two tuples                                            |                                               | SELECT tuplePlus((1, 2), (3, 4));                                                                    |
| url                    | table_macro | Performs queries against remote URLs using the specified format                              | Supports JSON, CSV, PARQUET, TEXT, BLOB       | SELECT * FROM url('https://urleng.com/test','JSON');                                                 |
| JSONExtract            | macro       | Extracts JSON data based on key from a JSON object                                           |                                               | SELECT JSONExtract(json_column, 'user.name');                                                        |
| JSONExtractString      | macro       | Extracts JSON data as a VARCHAR from a JSON object                                           |                                               | SELECT JSONExtractString(json_column, 'user.email');                                                 |
| JSONExtractUInt        | macro       | Extracts JSON data as an unsigned integer from a JSON object                                 |                                               | SELECT JSONExtractUInt(json_column, 'user.age');                                                     |
| JSONExtractInt         | macro       | Extracts JSON data as a 32-bit integer from a JSON object                                    |                                               | SELECT JSONExtractInt(json_column, 'user.balance');                                                  |
| JSONExtractFloat       | macro       | Extracts JSON data as a double from a JSON object                                            |                                               | SELECT JSONExtractFloat(json_column, 'user.score');                                                  |
| JSONExtractRaw         | macro       | Extracts raw JSON data based on key from a JSON object                                       |                                               | SELECT JSONExtractRaw(json_column, 'user.address');                                                  |
| JSONHas                | macro       | Checks if a JSON key exists and is not null                                                  |                                               | SELECT JSONHas(json_column, 'user.active');                                                          |
| JSONLength             | macro       | Returns the length of a JSON array                                                           |                                               | SELECT JSONLength(json_column, 'items');                                                             |
| JSONType               | macro       | Determines the type of JSON element at the given path                                        |                                               | SELECT JSONType(json_column, 'user.data');                                                           |
| JSONExtractKeys        | macro       | Extracts keys from a JSON object                                                             |                                               | SELECT JSONExtractKeys(json_column);                                                                 |
| JSONExtractValues      | macro       | Extracts all values as text from a JSON object                                               |                                               | SELECT JSONExtractValues(json_column);                                                               |
| equals                 | macro       | Checks if two values are equal                                                               |                                               | SELECT equals(column_a, column_b);                                                                   |
| notEquals              | macro       | Checks if two values are not equal                                                           |                                               | SELECT notEquals(column_a, column_b);                                                                |
| less                   | macro       | Checks if one value is less than another                                                     |                                               | SELECT less(column_a, column_b);                                                                     |
| greater                | macro       | Checks if one value is greater than another                                                  |                                               | SELECT greater(column_a, column_b);                                                                  |
| lessOrEquals           | macro       | Checks if one value is less than or equal to another                                         |                                               | SELECT lessOrEquals(column_a, column_b);                                                             |
| greaterOrEquals        | macro       | Checks if one value is greater than or equal to another                                      |                                               | SELECT greaterOrEquals(column_a, column_b);                                                          |
| dictGet                | macro       | Retrieves an attribute from a VARIABLE string or MAP                                         |                                               | SELECT dictGet('dictionary_name', 'attribute');                                                      |

###### Disclaimer
> DuckDB ® is a trademark of DuckDB Foundation. ClickHouse® is a trademark of ClickHouse Inc. All trademarks, service marks, and logos mentioned or depicted are the property of their respective owners. The use of any third-party trademarks, brand names, product names, and company names is purely informative or intended as parody and does not imply endorsement, affiliation, or association with the respective owners.
