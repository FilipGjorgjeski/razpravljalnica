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

func (s *Chat) GetView() tview.Primitive {
	return s.view
}

func (s *Chat) Update(data ChatData) {
	s.messageList.Clear()

	for i, message := range data.messages {
		s.addChatMessageEntry(message, i)
	}
}

func (s *Chat) addChatMessageEntry(message *razpravljalnica.Message, row int) {
	dateCell := tview.NewTableCell(message.GetCreatedAt().AsTime().Format(time.DateTime)).SetTextColor(tcell.ColorDarkGray)

	usernameCell := tview.NewTableCell(fmt.Sprintf("<%s>:", strconv.Itoa(int(message.GetUserId())))).SetAlign(tview.AlignRight)

	messageCell := tview.NewTableCell(message.GetText()).
		SetAlign(tview.AlignLeft).
		SetExpansion(1).
		SetReference(message)

	likesCell := tview.NewTableCell(fmt.Sprintf("♥ %d", message.GetLikes()))

	if s.d.selectedMessage != nil && s.d.selectedMessage.Id == message.Id {
		dateCell.SetBackgroundColor(colorFieldSelected)
		usernameCell.SetBackgroundColor(colorFieldSelected)
		messageCell.SetBackgroundColor(colorFieldSelected)
	}

	s.messageList.SetCell(row, 0, dateCell)
	s.messageList.SetCell(row, 1, usernameCell)
	s.messageList.SetCell(row, 2, messageCell)
	s.messageList.SetCell(row, 3, likesCell)
}

func (s *Chat) ResetMessageBox() {
	s.messageFormInput.SetText("")
	s.messageFormInputContent = ""
}

func (s *Chat) HideAndResetMessageBox() {
	s.view.AddItem(tview.NewBox(), 1, 0, 1, 1, 0, 0, false)
	s.ResetMessageBox()
}

func (s *Chat) ShowMessageBox() {
	s.view.AddItem(s.messageForm, 1, 0, 1, 1, 0, 0, true)
}

func (s *Chat) SetSelectable(value bool) {
	current, _ := s.messageList.GetSelectable()
	if current != value {
		s.messageList.SetSelectable(value, false)
	}
}
