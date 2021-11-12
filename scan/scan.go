package scan

import (
	"fmt"
	"log"

	"github.com/riete/db-compare/storage"

	"github.com/riete/db-compare/mysql"
)

type Scan struct {
	Mysql    mysql.MySql
	Database string
	Table    string
	HasPK    bool
	PKColumn string
	PKType   string
	Count    string
	Min      string
	Max      string
	Duration string
}

func (s *Scan) getMinMax() error {
	rows, _, err := s.Mysql.Query(
		s.Database,
		fmt.Sprintf("select ifnull(min(%s),0), ifnull(max(%s),0) from `%s`", s.PKColumn, s.PKColumn, s.Table),
	)
	if err != nil {
		return err
	}
	_, results, err := s.Mysql.ParseRows(rows)
	if err != nil {
		return err
	}
	s.Min, s.Max = results[0][0], results[0][1]
	return nil
}

func (s *Scan) diff() error {
	rows, duration, err := s.Mysql.Query(
		s.Database,
		fmt.Sprintf(
			"select count(*) from `%s` where `%s` > '%s'",
			s.Table,
			s.PKColumn,
			s.Max,
		),
	)
	if err != nil {
		return err
	}
	_, results, err := s.Mysql.ParseRows(rows)
	if err != nil {
		return nil
	}
	s.Count, s.Duration = results[0][0], duration
	return nil
}

func (s *Scan) countWithPK() error {
	if err := s.getMinMax(); err != nil {
		return err
	}
	rows, duration, err := s.Mysql.Query(
		s.Database,
		fmt.Sprintf(
			"select count(*) from `%s` where `%s` >= '%s' and `%s` <= '%s'",
			s.Table,
			s.PKColumn,
			s.Min,
			s.PKColumn,
			s.Max,
		),
	)
	if err != nil {
		return err
	}
	_, results, err := s.Mysql.ParseRows(rows)
	if err != nil {
		return nil
	}
	s.Count, s.Duration = results[0][0], duration
	return nil
}

func (s *Scan) countWithoutPK() error {
	rows, duration, err := s.Mysql.Query(s.Database, fmt.Sprintf("SELECT count(*) FROM `%s`", s.Table))
	if err != nil {
		return err
	}
	_, results, err := s.Mysql.ParseRows(rows)
	if err != nil {
		return err
	}
	s.Count, s.Duration = results[0][0], duration
	return nil
}

func (s *Scan) GetCount(pkScan bool) error {
	if s.HasPK {
		return s.countWithPK()
	} else if !pkScan {
		return s.countWithoutPK()
	}
	return nil
}

func (s *Scan) GetDiff(pkScan bool) error {
	if s.HasPK {
		return s.diff()
	} else if !pkScan {
		return s.countWithoutPK()
	}
	return nil
}

func LoadAndScan(ms mysql.MySql, pkScan bool) ([]Scan, error) {
	var scans []Scan
	_, results, err := storage.ReadFromExcel(storage.Filename, storage.CountSheet)
	if err != nil {
		return nil, err
	}
	for _, r := range results {
		hasPK := false
		if r[2] != "" {
			hasPK = true
		}
		s := Scan{
			Mysql:    ms,
			Database: r[0],
			Table:    r[1],
			HasPK:    hasPK,
			PKColumn: r[2],
			PKType:   r[3],
			Max:      r[5],
		}
		if err := s.GetDiff(pkScan); err != nil {
			return nil, err
		}
		log.Println(fmt.Sprintf("diff scan %s %s %s done", ms.Host, s.Database, s.Table))
		scans = append(scans, s)
	}
	return scans, nil
}

func SaveFull(srcScan, tgtScan []Scan) error {
	var data [][]string
	columns := []string{"database", "table", "pk_column", "pk_type", "min", "max", "src_count", "src_duration", "tgt_count", "tgt_duration", "equal"}
	for i := 0; i < len(srcScan); i++ {
		s := srcScan[i]
		t := tgtScan[i]
		equal := "false"
		if s.Count == t.Count {
			equal = "true"
		}
		data = append(
			data,
			[]string{s.Database, s.Table, s.PKColumn, s.PKType, s.Min, s.Max, s.Count, s.Duration, t.Count, t.Duration, equal},
		)
	}
	return storage.WriteToExcel(storage.Filename, storage.CountSheet, columns, data)
}

func SaveDiff(srcScan, tgtScan []Scan) error {
	var data [][]string
	columns := []string{"database", "table", "pk_column", "pk_type", "max", "src_count", "src_duration", "tgt_count", "tgt_duration", "equal"}
	for i := 0; i < len(srcScan); i++ {
		s := srcScan[i]
		t := tgtScan[i]
		equal := "false"
		if s.Count == t.Count {
			equal = "true"
		}
		data = append(
			data,
			[]string{s.Database, s.Table, s.PKColumn, s.PKType, s.Max, s.Count, s.Duration, t.Count, t.Duration, equal},
		)
	}
	return storage.WriteToExcel(storage.Filename, storage.DiffSheet, columns, data)
}
