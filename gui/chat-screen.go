package gui

import "github.com/rivo/tview"

func newChatScreen(header *Header, chat *Chat, sidebar *Sidebar) tview.Primitive {
	grid := getStandardGrid(true)

	return grid.
		AddItem(header.GetView(), 0, 0, 1, 2, 0, 0, false).
		AddItem(sidebar.GetView(), 1, 0, 1, 1, 0, 0, true).
		AddItem(chat.GetView(), 1, 1, 1, 1, 0, 0, false)
}
