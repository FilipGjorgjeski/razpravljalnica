package ui

import (
	"fmt"
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

	textInput.
		SetChangedFunc(func(text string) { chat.messageFormInputContent = text })

	messageForm := tview.NewForm().
		AddFormItem(textInput).
		AddButton("Send", func() {
			chat.d.conn.SendMessage(&razpravljalnica.PostMessageRequest{
				TopicId: chat.d.selectedTopic.Id,
				UserId:  chat.d.user.Id,
				Text:    chat.messageFormInputContent,
			})
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

	c.messageList.Select(len(data.messages)-1, 0)
}

func (c *Chat) addChatMessageEntry(message *razpravljalnica.Message, row int) {
	dateCell := tview.NewTableCell(message.GetCreatedAt().AsTime().Format(time.DateTime)).SetTextColor(tcell.ColorDarkGray)
	c.messageList.SetCell(row, 0, dateCell)

	user := c.d.conn.GetUserById(message.GetUserId())
	usernameCell := tview.NewTableCell(fmt.Sprintf("<%s>:", user.GetName())).SetAlign(tview.AlignRight)
	c.messageList.SetCell(row, 1, usernameCell)

	messageCell := tview.NewTableCell(message.GetText()).
		SetAlign(tview.AlignLeft).
		SetExpansion(1).
		SetReference(message)
	c.messageList.SetCell(row, 2, messageCell)
}

func (c *Chat) ResetMessageBox() {
	c.messageFormInput.SetText("")
	c.messageFormInputContent = ""
}

func (c *Chat) HideMessageBox() {
	c.view.AddItem(tview.NewBox(), 1, 0, 1, 1, 0, 0, false)
}

func (c *Chat) ShowMessageBox() {
	c.view.AddItem(c.messageForm, 1, 0, 1, 1, 0, 0, true)
}
