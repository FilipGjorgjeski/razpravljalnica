package ui

import "github.com/rivo/tview"

func newSidebar() tview.Primitive {
	return tview.NewTextView().SetText("Sidebar")
}
