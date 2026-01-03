package ui

import "github.com/rivo/tview"

func newHeader() tview.Primitive {
	return tview.NewTextView().SetLabel("Razpravljalnica Header")
}
