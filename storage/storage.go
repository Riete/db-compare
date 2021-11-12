package storage

import (
	"errors"
	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize"
)

const (
	Filename   = "scan.xlsx"
	PKSheet    = "table_pk"
	CountSheet = "table_full_count"
	DiffSheet  = "table_diff_count"
)

func NumberToExcelCell(num int, row int) string {
	var letter string
	var temp []int

	alphabet := []string{"", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
		"P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

	if num > 26 {
		for {
			k := num % 26
			if k == 0 {
				temp = append(temp, 26)
				k = 26
			} else {
				temp = append(temp, k)
			}
			num = (num - k) / 26
			if num <= 26 {
				temp = append(temp, num)
				break
			}
		}
		for _, value := range temp {
			letter = alphabet[value] + letter
		}
		return fmt.Sprintf("%s%d", letter, row)

	} else {
		return fmt.Sprintf("%s%d", alphabet[num], row)
	}
}

func WriteToExcel(filename, sheet string, columns []string, rows [][]string) error {
	xlsx, err := excelize.OpenFile(filename)
	if err != nil {
		xlsx = excelize.NewFile()
	}
	xlsx.NewSheet(sheet)
	xlsx.DeleteSheet("Sheet1")
	end := len(columns)
	for i := 0; i < end; i++ {
		xlsx.SetCellValue(sheet, NumberToExcelCell(i+1, 1), columns[i])
	}

	r := 2
	for _, row := range rows {
		for i := 0; i < end; i++ {
			xlsx.SetCellValue(sheet, NumberToExcelCell(i+1, r), row[i])
		}
		r += 1
	}
	if err := xlsx.SaveAs(filename); err != nil {
		return errors.New("导出失败: " + err.Error())
	}
	return nil
}

func ReadFromExcel(filename, sheet string) ([]string, [][]string, error) {
	xlsx, err := excelize.OpenFile(filename)
	if err != nil {
		return []string{}, [][]string{}, errors.New("文件不存在: " + err.Error())
	}
	rows := xlsx.GetRows(sheet)
	return rows[0], rows[1:], nil
}
