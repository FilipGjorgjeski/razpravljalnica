package gui

import (
	"fmt"
	"strconv"
	"time"

	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type Chat struct {
	view *tview.Grid

	d *Display

	messageList             *tview.Table
	messageForm             *tview.Form
	messageFormInput        *tview.InputField
	messageFormInputContent string
	highlightedRow          int
}

type ChatData struct {
	messages []*razpravljalnica.Message
}

func NewChat() *Chat {

	table := tview.NewTable().SetSelectable(true, false).SetBorders(true).
		SetSelectedStyle(tcell.StyleDefault.Background(tcell.ColorWhite).Foreground(tcell.ColorWhite))

	textInput := tview.NewInputField().
		SetLabel("Message").
		SetFieldWidth(0).
		SetAcceptanceFunc(tview.InputFieldMaxLength(100))

	grid := tview.NewGrid().SetColumns(0).SetRows(0, 5).
		AddItem(table, 0, 0, 1, 1, 0, 0, true)

	chat := &Chat{
		view:             grid,
		messageList:      table,
		messageFormInput: textInput,
	}

	table.SetSelectedFunc(func(row, column int) {
		message := table.GetCell(row, 2).Reference.(*razpravljalnica.Message)
		chat.d.ToMessageBox(message)
	}).SetSelectionChangedFunc(func(row, column int) {
		chat.highlightedRow = row
	})

	textInput.
		SetChangedFunc(func(text string) { chat.messageFormInputContent = text })

	messageForm := tview.NewForm().
		AddFormItem(textInput).
		AddButton("Send", func() {
			chat.d.sendOrUpdateMessage(chat.messageFormInputContent)
			chat.d.ToMessageList(nil)
		}).SetButtonsAlign(tview.AlignRight)

	chat.messageForm = messageForm

	return chat
}

func (c *Chat) GetView() tview.Primitive {
	return c.view
}

func (c *Chat) Update(data ChatData) {
	c.messageList.Clear()

	for i, message := range data.messages {
		c.addChatMessageEntry(message, i)
	}
}

func (c *Chat) addChatMessageEntry(message *razpravljalnica.Message, row int) {
	dateCell := tview.NewTableCell(message.GetCreatedAt().AsTime().Format(time.DateTime)).SetTextColor(tcell.ColorDarkGray)

	usernameCell := tview.NewTableCell(fmt.Sprintf("<%s>:", strconv.Itoa(int(message.GetUserId())))).SetAlign(tview.AlignRight)

	messageCell := tview.NewTableCell(message.GetText()).
		SetAlign(tview.AlignLeft).
		SetExpansion(1).
		SetReference(message)

	likesCell := tview.NewTableCell(fmt.Sprintf("♥ %d", message.GetLikes()))

	if c.d.selectedMessage != nil && c.d.selectedMessage.Id == message.Id {
		dateCell.SetBackgroundColor(colorFieldSelected)
		usernameCell.SetBackgroundColor(colorFieldSelected)
		messageCell.SetBackgroundColor(colorFieldSelected)
	}

	c.messageList.SetCell(row, 0, dateCell)
	c.messageList.SetCell(row, 1, usernameCell)
	c.messageList.SetCell(row, 2, messageCell)
	c.messageList.SetCell(row, 3, likesCell)
}

func (c *Chat) ResetMessageBox() {
	c.messageFormInput.SetText("")
	c.messageFormInputContent = ""
}

func (c *Chat) HideAndResetMessageBox() {
	c.view.AddItem(tview.NewBox(), 1, 0, 1, 1, 0, 0, false)
	c.ResetMessageBox()
}

func (c *Chat) ShowMessageBox() {
	c.view.AddItem(c.messageForm, 1, 0, 1, 1, 0, 0, true)
}

func (c *Chat) SetSelectable(value bool) {
	current, _ := c.messageList.GetSelectable()
	if current != value {
		c.messageList.SetSelectable(value, false)
	}
}

func (c *Chat) GetHighlightedMessage() *razpravljalnica.Message {
	return c.messageList.GetCell(c.highlightedRow, 2).Reference.(*razpravljalnica.Message)
}
