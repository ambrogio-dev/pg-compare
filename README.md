# pg-compare
PostgreSQL DDL comparing tool

[![NPM Version](http://img.shields.io/npm/v/pg-compare.svg?style=flat)](https://www.npmjs.org/package/pg-compare)
[![NPM Downloads](https://img.shields.io/npm/dm/pg-compare.svg?style=flat)](https://www.npmjs.org/package/pg-compare)

#### Why another diff-tool?

There are many db-difference tools, but all the open source tools are incomplete or discontinued,
and we needed an up-to-date tool for quickly do this operation in a single command line step.

## Npm Installation

A global install is prefered.

```
$ npm i -g pg-compare
```

## Run

```
$ pg-compare [PATH_TO_CONFIG]
```
where *PATH_TO_CONFIG* is the config file path (with all DBs parameters).

## What's new

**v. 0.1.1**: has been fixed a bug that for some tables duplicated column names in creating sql commands
**v. 0.1.1**: has been developed the possibility to compare data rows in tables to generate insert or delete queries

## How it works

You just need to prepare a correct config json with all dbs parameters and then run this module: it will generate all SQL commands for the requested parameters.  

**N.B.**  
The generated SQL is the delta from schema1 to schema2 (schema1 = schema2 + delta)

#### Config file format

The right format of the config file is the one shown in the example.json file. 

`connection1` *(mandatory)* Parameters for the connection to the db1  

- `host` *(mandatory)* Host name
- `user` *(mandatory)* Username
- `password` *(mandatory)* Password
- `database` *(mandatory)* Database name

`connection2` *(mandatory)* Parameters for the connection to the db2  

- `host` *(mandatory)* Host name
- `user` *(mandatory)* Username
- `password` *(mandatory)* Password
- `database` *(mandatory)* Database name

`compare:` *(mandatory)* Parameters for the comparing execution  

- `schema` *(mandatory)* Schema parameters  

  - `tables` Flag to enable table comparing (including columns, primary key and unique constraints)
  - `fkeys` Flag to enable foreign keys comparing
  - `functions` Flag to enable functions  comparing
  - `indexes` Flag to enable indexes comparing (excluding indexes generated for primary keys)
  - `types` Flag to enable enum types comparing 
  - `views` Flag to enable views comparing
  - `sequences` Flag to enable current value comparing for sequences (a sequence must exist on both databases)
  - `rows` Array of table names; for these there will be a rows comparison
- `options` Comparing options  

  - `mode` (**full** or **preserve**) If *full* check which items of schema2 are not present in schema1 and generate SQL commands to drop them.
  - `owner` Owner of created  DB items. If not present, it will be the **connection2.user** value.

`output` output file path with SQL generated commands. If not present, SQL commands will be written in the console output.  

`verbose` Flag to enable a verbose log during the comparing operation.

## Test

Tested on PostgreSQL 9.4.  
A test file is available with many test cases. We recommend you to try it on two empty DBs.

## Collaborate

Please feel free to report feedback or bugs or ask for new features.

## License

The MIT License (MIT)

Copyright (c) 2016 Ambrogio srl

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
