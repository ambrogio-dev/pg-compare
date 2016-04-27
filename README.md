# pg-compare
PostgreSQL DDL comparing tool

[![NPM Version](http://img.shields.io/npm/v/pg-compare.svg?style=flat)](https://www.npmjs.org/package/pg-compare)
[![NPM Downloads](https://img.shields.io/npm/dm/pg-compare.svg?style=flat)](https://www.npmjs.org/package/pg-compare)

#### Why another diff-tool?

There are many db-diffence tools, but all the open source tools are incomplete or abandoned, 
and we need an up-to-date tool for quickly do this operation, in a single command line step.

## Npm Installation

Install globally prefered

```
$ npm i -g pg-compare
```

## Run

```
$ pg-compare [configFilePath]
```

## How it works

You have only to prepare a correct config json with all dbs parameters and then run this module passing config file path; it will generate all sql commands for the requested parameters.  
**N.B.**  
The generated sql is the delta from schema1 to schema2 (schema1 = schema2 + delta)

#### Config file format

The right format of the config file is the one shown in the example.json file. Here the meaning of each key:  

**connection1:** *(mandatory)* Parameters for the connection to the db1
* **host:** *(mandatory)* Host name
* **user:** *(mandatory)* Username
* **password:** *(mandatory)* Password
* **database:** *(mandatory)* Database name

**connection2:** *(mandatory)* Parameters for the connection to the db2
* **host:** *(mandatory)* Host name
* **user:** *(mandatory)* Username
* **password:** *(mandatory)* Password
* **database:** *(mandatory)* Database name

**compare:** *(mandatory)* Parameters for the comparing execution
* **schema:** *(mandatory)* Schema parameters
  * **tables:** Flag that indicates if comparing each table (including columns, primary key and unique constraints)
  * **fkeys:** Flag that indicates if comparing foreign keys
  * **functions:** Flag that indicates if comparing functions
  * **indexes:** Flag that indicates if comparing indexes (excluding indexes generated for primary keys)
  * **types:** Flag that indicates if comparing enum types
  * **views:** Flag that indicates if comparing views
  * **sequences:** Flag that indicates if check current value for sequences that exist on both databases
* **options:** Comparing options
  * **mode:** (**full** or **preserve**) If full check items of schema2 not present in schema1 and generate drop commands
  * **owner:** Owner of DB items that will be created. If not present, it will be the **connection2.user** value

**output:** Path of the output file with sql commands that will be generated. If not present, sql commands will be written in console output  

**verbose:** Flag that indicates if write a verbose log during the compare operation

## Test

Tested on PostgreSQL 9.4.  
It is available a test file with functions to test a lot of different cases of dbs differences. We suggest to try it on two empty databases.

## Collaborate

Please feel free to report feedback or bugs or ask for new features.

## License

The MIT License (MIT)

Copyright (c) 2015 Ambrogio srl

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


TODO
Mettere roba in ordine di dichiarazione,
comparare check constraint
sincronizzare dati tabelle
comparare e creare sequence