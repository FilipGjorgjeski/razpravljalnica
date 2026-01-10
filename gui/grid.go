package gui

import "github.com/rivo/tview"

func getStandardGrid(sidebar bool) *tview.Grid {
	grid := tview.NewGrid().
		SetRows(3, 0, 3)

	if sidebar {
		grid.SetColumns(30, 0)
	} else {
		grid.SetColumns(0)
	}

	return grid.SetBorders(true)
}

func getCenteredFlexContainer(contents tview.Primitive, width, height int) *tview.Flex {
	return tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(contents, height, 1, true).
			AddItem(nil, 0, 1, false), width, 1, true).
		AddItem(nil, 0, 1, false)
}
