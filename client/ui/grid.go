package ui

import "github.com/rivo/tview"

func getStandardGrid() *tview.Grid {
	return tview.NewGrid().
		SetRows(3, 0, 3).
		SetColumns(30, 0, 30).
		SetBorders(true)
}
