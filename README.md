# Simple Typed SQL

JavaScript/TypeScript SQL query builder based on table-level abstraction with the goal of allowing one to write complex
SQL queries without needing use strings to refer to columns or tables.
Provides partial type safety when used with TypeScript.
Based on Knex.js.

Supports:
- Select, insert and update queries
- Complex where clauses
- Joins with complex join conditions
- Transactions
- Aggregation functions and group by

Upcoming:
- Select expressions

## Installation

```
npm install simple-typed-sql
```

## Config

Create Mapper object

```typescript
// Create knex connection
let knexClient = knex(...);

import * as sqlMapper from 'simple-typed-rpc'

let mapper = new sqlMapper.Mapper(knexClient);
```

Define mapping to a SQL table

```typescript
let fooMapping = sqlMapper.defineMapping(
    'foo_table_name',
    {
        id: sqlMapper.defineNumber(),
        name: sqlMapper.defineString(),
        createdTime: sqlMapper.defineDatetime({ fieldName: 'created_time' }),
        fooCount: sqlMapper.defineNumber({ fieldName: 'foo_count' })
    }
);
```

## Quick usage

Insert data:

```typescript
await mapper.insertInto(fooMapping, {
    id: 1,
    name: "foo1",
    createdTime: new Date(),
    fooCount: 5
});
```

Select all columns from table:

```typescript
let fooList = await mapper.selectAllFrom(fooMapping);

console.log(fooList[0].fooCount); // 5
/* Compile error in TypeScript:
console.log(fooList[0].nonExisting);
*/
```

Simple where clause

```typescript
await mapper.insertInto(fooMapping, {
    id: 2,
    name: "foo2",
    createdTime: new Date(),
    fooCount: 2
});

let littleFoos = await mapper
    .selectAllFrom(fooMapping)
    .whereLessThan(fooMapping.fooCount, 3);

console.log(littleFoos); /*
[{
    id: 2,
    name: "foo2",
    createdTime: "Sun Sep 25 2016 22:18:53 GMT+0300 (FLE Daylight Time)",
    fooCount: 2
}]*/
```

