package ui

import "github.com/rivo/tview"

type Header struct {
	view *tview.Grid

	statusTable *tview.Table
}

type HeaderData struct {
	username         string
	connectionStatus string
}

const headerText = `[yellow]Razpravljalnica

[white]PS Skupina 22`

func NewHeader() *Header {
	status := tview.NewTable().
		SetCellSimple(0, 0, "Username:").
		SetCellSimple(1, 0, "Connection status:")

	grid := tview.NewGrid().SetRows(0).SetColumns(0, 50).
		AddItem(tview.NewTextView().SetDynamicColors(true).SetText(headerText), 0, 0, 1, 1, 0, 0, false).
		AddItem(status, 0, 1, 1, 1, 0, 0, false)

	header := &Header{
		view:        grid,
		statusTable: status,
	}
	return header
}

func (c *Header) GetView() tview.Primitive {
	return c.view
}

func (h *Header) Update(data HeaderData) {
	h.statusTable.
		SetCellSimple(0, 1, data.username).
		SetCellSimple(1, 1, data.connectionStatus)
}
