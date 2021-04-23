import ts = require("typescript");
import { CodeWriter } from "./codewriter";
import * as fs from 'fs';
import * as path from 'path';
import { InterfaceDeclaration } from "typescript";
import { Emitter } from "./emitter";

type DBType = 'world'|'auth'|'characters'

const MethodTypes = 
{
    'string': 0,
    'uint8': 1,
    'uint16': 2,
    'uint32': 3,
    'uint64': 4,
    'int8': 5,
    'int16': 6,
    'int32': 7,
    'int64': 8,
    'float': 9,
    'double': 10,
    'int': 11,
}

type FieldType = keyof typeof MethodTypes

class Field {
    name: string;
    type: FieldType;
    initialization: string;
    isPrimaryKey: boolean;

    sqlInitialization() {
        return this.type == 'string' ? `\\${this.initialization.slice(0,this.initialization.length-1)}\\"` : this.initialization
    }

    constructor(name: string, type: FieldType, isPrimaryKey: boolean, initialization: string) {
        this.name = name;
        this.type = type;
        this.initialization = initialization;
        this.isPrimaryKey = isPrimaryKey;
    }
}

class DBDictionary {
    name: string;
    keyType: FieldType;
    valueType: FieldType;
    owner: Entry;

    constructor(owner: Entry, name: string, keyType: FieldType, valueType: FieldType) {
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.owner = owner;
    }

    get db_name() {
        return `${this.owner.className.toLowerCase()}_${this.name.toLowerCase()}`
    }
}

class EntryBase {
    className: string;
    fields: Field[] = [];
    databaseType: DBType;

    constructor(className: string) {
        this.className = className;
    }
}

class SubEntry extends EntryBase {
    constructor(owner: string, name: string) {
        super(owner+"_"+name);
    }
}

class Entry extends EntryBase {
    databaseType: DBType;
    dictionaries: DBDictionary[] = []
    
    constructor(className: string, databaseType: DBType) {
        super(className);
        this.databaseType = databaseType;
    }
}

let subentries: SubEntry[] = [];
let entries: Entry[] = [];
let baseClassMap: {[key:string]:string} = {}

function wrap(str: string, is_string: boolean) {
    return is_string ? `+ "\"" + std::string(${str}) + "\""` : `std::to_string(${str})`
}

export function handleClass(node: ts.ClassDeclaration, emitter: Emitter) {
    const writer = emitter.writer;
    if(!node.decorators) {
        return;
    }

    const className = node.name.getText(node.getSourceFile());
    
    let entry: Entry|undefined = undefined;
    node.decorators.forEach((x)=>{
        const ft = x.getText(x.getSourceFile());
        switch(ft) {
            case '@WorldTable':
            entry = new Entry(className,'world');
            break;
            case '@AuthTable':
            entry = new Entry(className,'auth');
            break;
            case '@CharactersTable':
            entry = new Entry(className,'characters');
            break;
            default:
        }
    });
    
    if(!entry) {
        return;
    }
    
    node.members.forEach((memberRaw)=>{
        if(memberRaw.kind!==ts.SyntaxKind.PropertyDeclaration) {
            return;
        }
        
        const member = memberRaw as ts.PropertyDeclaration;
        
        let isField = false;
        let isPrimaryKey = false;
        if(member.decorators) {
            member.decorators.forEach((deco)=>{
                if(deco.getText(deco.getSourceFile())=='@Field') {
                    isField = true;
                }

                if(deco.getText(deco.getSourceFile())=='@PrimaryKey') {
                    isPrimaryKey = true;
                    isField = true;
                }
            });
        }
        
        if(!isField) {
            return;
        }

        const name = member.name.getText(member.getSourceFile());
        const type = member.type.getText(member.getSourceFile()) as FieldType;
        if(type.startsWith('TSDBDict')) {
            let typeargs = type.split('<')[1].split('>')[0].split(',');
            for(const ta of typeargs) {
                if(!Object.keys(MethodTypes).includes(ta)) {
                    throw new Error(`Invalid type for database dict: ${ta}`);
                }
            }
            entry.dictionaries.push(new DBDictionary(
                entry
                , name
                , typeargs[0] as FieldType
                , typeargs[1] as FieldType
                ));
        } else {
            if(!Object.keys(MethodTypes).includes(type)) {
                throw new Error(`Invalid type for database field: ${type}`);
            }

            if(type=='string'&&isPrimaryKey) {
                throw new Error(`Strings cannot be primary keys (yet)`);
            }
            
            if(!member.initializer) {
                throw new Error(`Database fields must be initialized (= something in the declaration)`);
            }
            const defValue = member.initializer.getText(member.getSourceFile());

            entry.fields.push(new Field(name,type,isPrimaryKey,defValue));
        }

    });

    const pks = entry.fields.filter(x=>x.isPrimaryKey);

    if(pks.length===0) {
        throw new Error(`Database rows must have at least one primary key.`)
    }

    entries.push(entry);

    writer.writeString('\n\n    ');
    writer.writeString('TSString loadQuery()');
    writer.BeginBlock();
    writer.writeString(`return JSTR("SELECT * FROM \`${entry.className}\` WHERE `);
    writer.writeString(
        pks.map(x=>`\`${x.name}\` = ")+this->${x.name}+JSTR("`)
            .join(' AND '));

    writer.writeString(';");');
    writer.EndBlock();

    writer.writeString('\n    ');
    writer.writeString('TSString saveQuery()');
    writer.BeginBlock();
    writer.writeString(`return JSTR("INSERT INTO \`${entry.className.toLowerCase()}\` VALUES ( `);
    writer.writeString(
        entry.fields.map(x=>{
            let str = "";
            if(x.type==='string') {
                str+=`\\"") + this->${x.name} + JSTR("\\"`
            } else {
                str+=`") + this->${x.name} + JSTR("`;
            }
            return str;
        }).join(' , ')
    )
    writer.writeString(') ON DUPLICATE KEY UPDATE ');
    writer.writeString(
        entry.fields.map(x=>{
            let str =`\`${x.name}\` = `;
            if(x.type==='string') {
                str+='\\"'
            }
            str+=`") + this->${x.name} + JSTR("`

            if(x.type==='string') {
                str+='\\"';
            }

            return str;
        }).join(' , ')
    )

    writer.writeString(';");')
    writer.EndBlock();

    writer.writeString('\n');

    writer.writeString('    TSString removeQuery() ');
    writer.BeginBlock();
    writer.writeString(`return JSTR("DELETE FROM \`${entry.className.toLowerCase()}\` WHERE `);
    writer.writeString(
        pks.map(x=>`\`${x.name}\` = ")+this->${x.name}+JSTR("`)
            .join(' AND '));
    writer.writeString(';");');
    writer.EndBlock();

    const queryType = entry.databaseType == 'world' ? 'QueryWorld' : 
        entry.databaseType == 'auth' ? 'QueryAuth' : 'QueryCharacters';

    writer.writeStringNewLine(``)
    writer.writeString(`void save()`)
    writer.BeginBlock();
    writer.writeStringNewLine(`${queryType}(saveQuery());`);
    writer.writeStringNewLine(``)
    entry.dictionaries.forEach(x=>{
        writer.writeStringNewLine(`for(auto itr = ${x.name}->map_begin(); itr != ${x.name}->map_end(); ++itr)`)
        writer.BeginBlock();
        writer.writeStringNewLine(`if(!itr->second._dirty) continue;`)
        writer.writeStringNewLine(
              `${queryType}(JSTR("INSERT INTO \`${x.db_name}\` VALUES (`
            + `${entry.fields
                    .filter(x=>x.isPrimaryKey)
                    .map(x=>`" + ${wrap(x.name,x.type==='string')} + "`)
                }`
            + `, " + ${wrap('itr->first',x.keyType==='string')} + " , " + ${wrap('itr->second._value',x.valueType==='string')} + "`
            + `)`
            + ` ON DUPLICATE KEY UPDATE `
            + ` \`map_value\` = " + `
            + wrap('itr->second._value',x.valueType=='string')
            + ` + ";"));`)
        writer.EndBlock();
        writer.writeStringNewLine(``)
        writer.writeStringNewLine(`for(auto itr = ${x.name}->erases_begin(); itr != ${x.name}->erases_end(); ++itr)`)
        writer.BeginBlock();
        writer.writeStringNewLine(
              `${queryType}(JSTR("`
            + `DELETE FROM \`${x.db_name}\``
            + ` WHERE `
            + `${entry.fields
                .filter(x=>x.isPrimaryKey)
                .map(x=>`\`${x.name}\` = " + ${wrap(x.name,x.type==='string')} + "`)
                .join(' AND ')
            }`
            + ` AND \`map_key\` = " + ${wrap('*itr',x.keyType==='string')} + "`
            + `;"));`)
        writer.EndBlock();
        writer.writeStringNewLine(``)
        writer.writeStringNewLine(`${x.name}->clear();`)
    });
    writer.EndBlock();
    writer.writeStringNewLine(``)
    writer.writeString(`void remove()`);
    writer.BeginBlock();
    writer.writeStringNewLine(`${queryType}(removeQuery());`)
    writer.EndBlock();
    writer.writeStringNewLine(``)
    writer.writeString(`static TSString LoadQuery(TSString query)`)
    writer.BeginBlock();
    writer.writeStringNewLine(`return JSTR("SELECT * from ${entry.className.toLowerCase()} WHERE ") + query + JSTR(";");`)
    writer.EndBlock();

    writer.writeString(`\n    static TSArray<std::shared_ptr<${entry.className}>> Load(TSString query)`);
    writer.BeginBlock();
    writer.writeStringNewLine(`auto arr = TSArray<std::shared_ptr<${entry.className}>>{};`);
    writer.writeStringNewLine(`auto res = ${queryType}(LoadQuery(query));`)
    writer.writeStringNewLine(`while(res->GetRow())`)
    writer.BeginBlock();
    writer.writeStringNewLine(`auto obj = std::make_shared<${entry.className}>();`);
    entry.fields.forEach((v,i)=>{
        const resolveType = ()=> {
            switch(v.type) {
                case 'double': return 'GetDouble';
                case 'float': return 'GetFloat';
                case 'int8': return 'GetInt8';
                case 'int16': return 'GetInt16';
                case 'int32': return 'GetInt32';
                case 'int64': return 'GetInt64';
                case 'uint8': return 'GetUInt8';
                case 'uint16': return 'GetUInt16';
                case 'uint32': return 'GetUInt32';
                case 'uint64': return 'GetUInt64';
                case 'int': return 'GetInt32';
                case 'string': return 'GetString';
            }
        }
        writer.writeStringNewLine(`obj->${v.name} = res->${resolveType()}(${i});`)
    });

    entry.dictionaries.forEach(x=>{
        writer.writeStringNewLine(``)
        writer.writeStringNewLine(`// read ${x.name}`);
        const v = `${x.name}_res`
        writer.writeStringNewLine(
              `auto ${v} =`
            + ` ${queryType}("SELECT * from \`${x.db_name}\` WHERE`
            + ` ${
                entry.fields
                    .filter(x=>x.isPrimaryKey)
                    .map(x=>`\`${x.name}\` = " + ${wrap(`obj->${x.name}`,x.type=='string')} + "`)
                    .join(' AND ')
            }`
            + `;");`);
        
        writer.writeStringNewLine(`while(${v}->GetRow())`)
        writer.BeginBlock();
        const vo = (type: FieldType)=>{
            switch(type) {
                case 'string': return `GetString`
                case 'double': return `GetDouble`
                case 'float': return `GetFloat`
                case 'int': return `GetInt32`
                case 'int8': return `GetInt8`
                case 'uint8': return `GetUInt8`
                case 'int16': return `GetInt16`
                case 'uint16': return `GetUInt16`
                case 'uint32': return `GetUInt32`
                case 'int32': return `GetInt32`
                case 'int64': return `GetInt64`
                case 'uint64': return `GetUInt64`
                default:
                    throw Error(`Invalid field type: ${type}`);
            }
        };

        const pk_count = entry.fields.filter(x=>x.isPrimaryKey).length;
        writer.writeStringNewLine(`obj->${x.name}->set_silent(${v}->${vo(x.keyType)}(${pk_count}),${v}->${vo(x.valueType)}(${pk_count+1}));`)
        writer.EndBlock();
    });

    writer.writeStringNewLine(`arr.push(obj);`);
    writer.EndBlock();
    writer.writeStringNewLine(`return arr;`);
    writer.EndBlock();
    writer.writeStringNewLine(``)

    const constructor = node.members.find((x)=>x.kind==ts.SyntaxKind.Constructor) as ts.ConstructorDeclaration;
    if(constructor!==undefined) {
        // add a default constructor if there is none already
        if(constructor.parameters.length>0) {
            const name = baseClassMap[entry.className];
            writer.writeStringNewLine(`${entry.className}() : ${name}() {}`)
        }
    }
}

export function setBaseClass(node: ts.ClassDeclaration | InterfaceDeclaration, cls: string) {
    if(!node.name) {
        return;
    }
    const nodename = node.name.getText(node.getSourceFile());

    baseClassMap[nodename] = cls;
}

export function writeIncludeTableCreator(writer: CodeWriter) {
    writer.writeStringNewLine('#include "TableCreator.h"')
}

export function writeTableCreationCall(writer: CodeWriter) {
    writer.writeStringNewLine('    WriteTables();');
}

const getReadSQLType = (field: Field)=>{
    switch(field.type) {
        case 'int': return 'INT(11)'
        case 'int8': return 'TINYINT(4)'
        case 'int16': return 'SMALLINT(6)'
        case 'int32': return 'INT(11)'
        case 'int64': return 'BIGINT(20)'
        case 'uint8': return 'TINYINT(3) UNSIGNED'
        case 'uint16': return 'SMALLINT(5) UNSIGNED'
        case 'uint32': return 'INT(10) UNSIGNED'
        case 'uint64': return 'BIGINT(20) UNSIGNED'
        case 'float': return 'FLOAT'
        case 'double': return 'DOUBLE'
        case 'string': return 'TEXT'
    }
}

const getWriteSQLType = (field: Field)=>{
    switch(field.type) {
        case 'int': return 'INT'
        case 'int8': return 'TINYINT'
        case 'int16': return 'SMALLINT'
        case 'int32': return 'INT'
        case 'int64': return 'BIGINT'
        case 'uint8': return 'TINYINT UNSIGNED'
        case 'uint16': return 'SMALLINT UNSIGNED'
        case 'uint32': return 'INT UNSIGNED'
        case 'uint64': return 'BIGINT UNSIGNED'
        case 'float': return 'FLOAT'
        case 'double': return 'DOUBLE'
        case 'string': return 'TEXT'
    }
}

function tableGeneration(writer: CodeWriter, tableName: string, dbtype: DBType, fields: Field[]) {
    writer.BeginBlock();
    writer.writeStringNewLine(`// ${tableName}`)
    let dbid = dbtype
          .slice(0,1).toUpperCase()
        + dbtype.slice(1)

    writer.writeStringNewLine(`bool should_create = true;`)

    writer.writeStringNewLine(
        `auto db = ${dbid}DatabaseInfo()->Database().std_str();`)
    writer.writeString(
        `auto rows = QueryWorld(JSTR(`)
    writer.writeStringNewLine(
          `"SELECT * from \`information_schema\`.\`COLUMNS\``
        + ` WHERE \`TABLE_SCHEMA\`= \\""+ db + "\\"`
        + ` AND \`TABLE_NAME\` = \\"${tableName}\\";"));`)

    writer.writeString(
        `if(rows->GetRow())`)
    writer.BeginBlock();
    writer.writeStringNewLine(`should_create = false;`);
    fields.forEach((x,i)=>{
        writer.writeStringNewLine(`bool found_${x.name} = false;`);
    });

    writer.writeString('do ');
    writer.BeginBlock();
    writer.writeStringNewLine(
        `auto column = rows->GetString(COLUMN_NAME_INDEX).std_str();`)

    writer.writeStringNewLine(
          `auto was_pk = QueryWorld(JSTR(`
        + ` "SELECT * from \`information_schema\`.\`KEY_COLUMN_USAGE\``
        + ` WHERE \`CONSTRAINT_SCHEMA\` = \\"\"+db+\"\\"`
        + ` and \`TABLE_NAME\` = \\"${tableName}\\"`
        + ` and \`COLUMN_NAME\` = \\"\"+column+\"\\"`
        + ` ;"))->GetRow();`);

    fields.forEach((x,i)=>{
        if(i==0) writer.writeString(`if `)
        else writer.writeString(` else if `)

        writer.writeString(
            `(column == "${x.name}")`)
        writer.BeginBlock();

        writer.writeStringNewLine(
            `found_${x.name} = true;`);

        writer.writeStringNewLine(
            `auto type = rows->GetString(COLUMN_TYPE_INDEX).std_str();`);

        writer.writeStringNewLine(
            `std::transform(type.begin(), type.end(), type.begin(), std::toupper);`
        );

        writer.writeString(
            `if (type != "${getReadSQLType(x)}")`)
        writer.BeginBlock();
        // mismatch + we're a string = we have to remove and add again
        if(x.type=='string') {
            writer.writeStringNewLine(`ask("${tableName}:"+column+" changed type from "+type+" to ${x.type}");`);
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` DROP \`\"+column+\"\`;"));`)
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` ADD \`\"+column+\"\` TEXT;"));`)
        } else {
            writer.writeStringNewLine(`if (type == "TEXT")`)
            writer.BeginBlock();
            writer.writeStringNewLine(`ask("${tableName}:"+column+" changed type from "+type+" to ${x.type}");`);
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` DROP \`\"+column+\"\`;"));`)
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` ADD \`\"+column+\"\` ${getWriteSQLType(x)};"));`)
            writer.EndBlock(false);
            writer.writeString(` else `);
            writer.BeginBlock();

            writer.writeString(`if (was_pk) `)
            writer.BeginBlock();
            writer.writeStringNewLine(`ask("${tableName}:"+column+" changed type from "+type+" to ${x.type} and was a primary key (whole db will be destroyed)");`);
            writer.writeStringNewLine(`should_create = true;`);
            writer.writeStringNewLine(`break;`)
            writer.EndBlock();
            writer.writeStringNewLine(`ask("${tableName}:"+column+" changed type from "+type+" to ${x.type}");`);
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\``
                + ` MODIFY \`${x.name}\` ${getWriteSQLType(x)}`
                + `;"));`);
            writer.EndBlock();
        }

        writer.EndBlock();
        writer.EndBlock(true);
    });

    writer.writeString(
        ' else ')
    writer.BeginBlock();
    writer.writeStringNewLine(`ask("${tableName}:"+column+" was removed");`);
    writer.writeStringNewLine(
        `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` DROP \`"+rows->GetString(COLUMN_NAME_INDEX)+"\`;"));`)
    writer.EndBlock();


    writer.EndBlock(true);
    writer.writeStringNewLine(
        ' while(rows->GetRow());')
    fields.forEach((x,i)=>{
        writer.writeString(
            `if( !should_create && !found_${x.name} )`);
        writer.BeginBlock();
        if(x.isPrimaryKey) {
            writer.writeStringNewLine(`ask("${tableName}: new primary key ${x.name} missing, need to rebuild database.");`);
            writer.writeStringNewLine(`should_create = true;`);
        } else {
            writer.writeStringNewLine(
                `Query${dbid}(JSTR("ALTER TABLE \`${tableName}\` ADD \`${x.name}\` ${getWriteSQLType(x)};"));`)
        }
        writer.EndBlock();
    });

    writer.EndBlock();
    writer.writeString(
        `if (should_create)`
    )
    writer.BeginBlock();
    writer.writeStringNewLine(`Query${dbid}(JSTR("DROP TABLE IF EXISTS \`${tableName}\`;"));`)
    writer.writeString(
        `Query${dbid}(JSTR("CREATE TABLE \`${tableName}\` (`);
    fields.forEach((field,index,arr)=>{
        writer.writeString(
            `\`${field.name}\` ${getWriteSQLType(field)}, `);
    });
    writer.writeString('PRIMARY KEY (')
    fields.filter(x=>x.isPrimaryKey).forEach((field,i,arr)=>{
        writer.writeString(`${field.name}`)
        if(i<arr.length-1) {
            writer.writeString(',');
        }
    });
    writer.writeStringNewLine('));"));');
    writer.EndBlock();
    fields.filter(x=>!x.isPrimaryKey).forEach(x=>{
        writer.writeStringNewLine(
            `Query${dbid}(JSTR("UPDATE \`${tableName}\` SET ${x.name} = ${x.sqlInitialization()} WHERE ${x.name} IS NULL;"));`)
    });
    writer.EndBlock();
}

export function writeTableCreationFile(outDir: string) {
    const writer = new CodeWriter();

    writer.writeStringNewLine('#include "TSDatabase.h"')
    writer.writeStringNewLine('#include <fstream>')
    writer.writeStringNewLine('#include <iostream>')
    writer.writeStringNewLine('#include <algorithm>')
    writer.writeStringNewLine('#include <string>')
    writer.writeStringNewLine('#include <cstdlib>')
    writer.writeStringNewLine(`#define COLUMN_NAME_INDEX 3`)
    writer.writeStringNewLine(`#define COLUMN_TYPE_INDEX 15`)

    writer.writeStringNewLine('');
    writer.writeString(`void ask(std::string msg)`)
    writer.BeginBlock();
    writer.writeStringNewLine(`std::cout << msg << ", this is a destructive operation.\\n";`);
    writer.EndBlock();
    writer.writeString('void WriteTables()')
    writer.BeginBlock();
    entries.forEach((entry)=>{
        tableGeneration(writer,entry.className,entry.databaseType,entry.fields);
        entry.dictionaries.forEach(x=>{
            let fields: Field[] = [];
            fields = fields.concat(entry.fields.filter(x=>x.isPrimaryKey))
            fields.push(new Field('map_key',x.keyType,true,''));
            fields.push(new Field('map_value',x.valueType,false,x.valueType==='string' ? '""':'0'));
            tableGeneration(writer,x.db_name,entry.databaseType,fields);
        });
    });

    writer.EndBlock();

    const tableFile = path.join(outDir,'livescripts','TableCreator.cpp');
    fs.writeFileSync(tableFile,writer.getText());
}