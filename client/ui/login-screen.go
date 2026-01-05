package ui

import (
	"github.com/rivo/tview"
)

func newLoginScreen(d *Display, header *Header) tview.Primitive {
	username := ""

	loginForm := tview.NewForm().
		AddInputField("Username", "", 20, nil, func(text string) { username = text }).
		AddButton("Submit", func() {
			d.SetUsername(username)
			d.ToTopicMenu()
		})

	loginForm.SetBorder(true)

	centeredLoginForm := getCenteredFlexContainer(loginForm, 40, 7)

	grid := getStandardGrid(false)

	return grid.
		AddItem(header.GetView(), 0, 0, 1, 1, 0, 0, false).
		AddItem(centeredLoginForm, 1, 0, 1, 1, 0, 100, true)
}
