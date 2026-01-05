package ui

import "github.com/rivo/tview"

func newChatScreen(d *Display, header *Header, chat *Chat) tview.Primitive {
	grid := getStandardGrid(true)

	return grid.
		AddItem(header.GetView(), 0, 0, 1, 2, 0, 0, false).
		AddItem(newSidebar(d), 1, 0, 1, 1, 0, 0, true).
		AddItem(chat.GetView(), 1, 1, 1, 1, 0, 0, false)
}
