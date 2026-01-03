package ui

import (
	"github.com/rivo/tview"
)

func newLoginScreen() tview.Primitive {
	loginForm := tview.NewForm().
		AddInputField("Username", "", 20, nil, func(text string) {}).
		AddButton("Submit", func() { pages.SwitchToPage("chat") })

	grid := getStandardGrid()

	return grid.
		AddItem(newHeader(), 0, 0, 1, 3, 0, 0, false).
		AddItem(loginForm, 1, 1, 1, 1, 0, 100, true)
}
