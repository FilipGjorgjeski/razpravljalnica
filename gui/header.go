package gui

import (
	"fmt"

	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/rivo/tview"
)

type Header struct {
	view *tview.Grid

	statusTable *tview.Table
}

type HeaderData struct {
	username           string
	clusterState       string
	err                string
	displayMode        DisplayMode
	highlightedMessage *razpravljalnica.Message
}

const headerText = `[yellow]Razpravljalnica

[white]PS Skupina 22`

func NewHeader() *Header {
	status := tview.NewTable().
		SetCellSimple(0, 0, "Username:").
		SetCellSimple(1, 0, "Cluster state:").
		SetCellSimple(2, 0, "Error:").
		SetCellSimple(3, 0, "DisplayMode:").
		SetCellSimple(4, 0, "Highlighted message:")

	grid := tview.NewGrid().SetRows(0).SetColumns(30, 0).
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
		SetCellSimple(1, 1, data.clusterState).
		SetCellSimple(3, 1, string(data.displayMode))

	if data.highlightedMessage != nil {
		h.statusTable.SetCellSimple(4, 1, fmt.Sprintf("{%d} <%d>: %s likes=%d liked=%t", data.highlightedMessage.Id, data.highlightedMessage.UserId, data.highlightedMessage.Text, data.highlightedMessage.Likes, data.highlightedMessage.LikedByUser))
	}
	if data.err != "" {
		h.statusTable.SetCellSimple(2, 1, data.err)
	}
}
