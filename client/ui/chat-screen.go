package ui

import "github.com/rivo/tview"

func newChatScreen() tview.Primitive {
	grid := getStandardGrid()

	return grid.
		AddItem(newHeader(), 0, 0, 1, 3, 0, 0, false).
		AddItem(newSidebar(), 1, 0, 1, 1, 0, 0, false)
}
