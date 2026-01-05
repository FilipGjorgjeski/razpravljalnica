package ui

import (
	"fmt"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/client/connection"
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type Chat struct {
	view *tview.Table
}

type ChatData struct {
	messages []*razpravljalnica.Message
}

func NewChat() *Chat {

	table := tview.NewTable().SetSelectable(true, false).SetBorders(true).
		SetSelectedStyle(tcell.StyleDefault.Background(tcell.ColorWhite).Foreground(tcell.ColorWhite))

	chat := &Chat{
		view: table,
	}
	return chat
}

func (c *Chat) GetView() tview.Primitive {
	return c.view
}

func (c *Chat) Update(data ChatData) {
	c.view.Clear()

	for i, message := range data.messages {
		c.addChatMessageEntry(message, i)
	}

	c.view.Select(len(data.messages)-1, 0)
}

func (c *Chat) addChatMessageEntry(message *razpravljalnica.Message, row int) {
	dateCell := tview.NewTableCell(message.GetCreatedAt().AsTime().Format(time.DateTime)).SetTextColor(tcell.ColorDarkGray)
	c.view.SetCell(row, 0, dateCell)

	user := connection.GetUserById(message.GetUserId())
	usernameCell := tview.NewTableCell(fmt.Sprintf("<%s>:", user.GetName())).SetAlign(tview.AlignRight)
	c.view.SetCell(row, 1, usernameCell)

	messageCell := tview.NewTableCell(message.GetText()).SetAlign(tview.AlignLeft).SetExpansion(1)
	c.view.SetCell(row, 2, messageCell)
}
