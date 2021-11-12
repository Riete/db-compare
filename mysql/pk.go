package mysql

import (
	"fmt"

	"github.com/riete/db-compare/storage"
)

type Table struct {
	Name     string
	HasPK    bool
	PKColumn string
	PKType   string
}

type Database struct {
	Name   string
	Tables []Table
}

type PK struct {
	MySql     MySql
	Databases []Database
}

func (pk PK) parseTablePK(database string) ([]Table, error) {
	// table_schema|table_name|column_name|data_type
	// db_1        |table_1   |column_1   |int
	var table []Table
	statement := fmt.Sprintf(`select distinct tb.table_schema,tb.table_name,tbpri.column_name,tbpri.data_type
from
(select table_schema,table_name from information_schema.tables where table_schema = '%s') tb
left join
(select table_schema,table_name,column_name,data_type from information_schema.COLUMNs  where table_schema = '%s' and COLUMN_key = 'PRI'
 and table_name not in (select table_name from information_schema.COLUMNs  where table_schema = '%s' and COLUMN_key = 'PRI' group by table_schema,table_name having count(1)>1)
) tbpri
on tb.table_schema = tbpri.table_schema and tb.table_name = tbpri.table_name order by tb.table_name;`, database, database, database)
	rows, _, err := pk.MySql.Query(database, statement)
	if err != nil {
		return nil, err
	}
	_, results, err := pk.MySql.ParseRows(rows)
	if err != nil {
		return nil, err
	}
	for _, r := range results {
		hasPK := false
		if r[2] != "" {
			hasPK = true
		}
		table = append(table, Table{Name: r[1], HasPK: hasPK, PKColumn: r[2], PKType: r[3]})
	}
	return table, nil
}

func (pk PK) Save() error {
	var data [][]string
	columns := []string{"database", "table", "pk_column", "pk_type"}
	for _, database := range pk.Databases {
		for _, table := range database.Tables {
			data = append(data, []string{database.Name, table.Name, table.PKColumn, table.PKType})
		}
	}
	return storage.WriteToExcel(storage.Filename, storage.PKSheet, columns, data)
}

func (pk *PK) Parse() error {
	databases, err := pk.MySql.GetDatabases()
	if err != nil {
		return err
	}
	for _, d := range databases {
		database := Database{Name: d}
		tables, err := pk.parseTablePK(d)
		if err != nil {
			return nil
		}
		database.Tables = tables
		pk.Databases = append(pk.Databases, database)
	}
	return nil
}
