package ui

import (
	"github.com/FilipGjorgjeski/razpravljalnica/client/connection"
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type DisplayMode int

const (
	UsernameSelection DisplayMode = iota
	TopicMenu
	MessageNew
	MessageList
	MessageEdit
)

type Display struct {
	user          *razpravljalnica.User
	activeMode    DisplayMode
	selectedTopic *razpravljalnica.Topic

	chat   *Chat
	header *Header
	pages  *tview.Pages
	app    *tview.Application
}

func NewDisplay(app *tview.Application, header *Header, chat *Chat, pages *tview.Pages, defaultMode DisplayMode) *Display {
	return &Display{
		activeMode:    defaultMode,
		selectedTopic: &razpravljalnica.Topic{},

		pages:  pages,
		header: header,
		chat:   chat,
		app:    app,
	}
}

func (d *Display) RegisterKeyboardHandlers() {
	d.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			switch d.activeMode {
			case UsernameSelection, TopicMenu:
				d.app.Stop()
			case MessageList:
				d.ToTopicMenu()
			case MessageNew:
				d.ToMessageList(nil)
			}
		}
		return event
	})

	// Selecting a message from the list
	d.chat.messageList.SetSelectedFunc(func(row, column int) {
		message := d.chat.messageList.GetCell(row, 2).Reference.(*razpravljalnica.Message)
		if message.UserId == d.user.Id {
			d.ToMessageBox(message)
		} else {
			d.ToMessageBox(nil)
		}
	})
}

func (d *Display) Update() {
	// Handle page
	if d.activeMode == UsernameSelection {
		d.pages.SwitchToPage(string(LoginPage))
	} else {
		d.pages.SwitchToPage(string(ChatPage))
	}

	d.header.Update(HeaderData{
		username:      d.user.GetName(),
		selectedTopic: d.selectedTopic.GetName(),
	})

	messages := connection.GetTopicMessages(d.selectedTopic.GetId())

	d.chat.Update(ChatData{
		messages: messages,
	})
}

func (d *Display) GetSelectedTopic() *razpravljalnica.Topic {
	return d.selectedTopic
}

func (d *Display) GetUsername() string {
	return d.user.GetName()
}

func (d *Display) SetUsername(username string) {
	d.user = &razpravljalnica.User{
		Name: username,
		Id:   1,
	}
	d.Update()
}

func (d *Display) ToTopicMenu() {
	d.activeMode = TopicMenu
	d.selectedTopic = &razpravljalnica.Topic{}
	d.chat.messageList.SetSelectable(false, false)
	d.Update()
}

// Focus message list of specific topic, if topic is nil, current active topic will not be changed
func (d *Display) ToMessageList(topic *razpravljalnica.Topic) {
	if topic != nil {
		d.selectedTopic = topic
	}
	d.activeMode = MessageList
	d.chat.messageList.SetSelectable(true, false)
	d.chat.HideMessageBox()
	d.chat.ResetMessageBox()
	d.Update()
	d.app.SetFocus(d.chat.GetView())
}

// Focus message box, editing message if provided
func (d *Display) ToMessageBox(selectedMessage *razpravljalnica.Message) {
	if selectedMessage == nil {
		d.chat.messageFormInput.SetLabel("New message")
	} else {
		d.chat.messageFormInput.SetLabel("Edit message")
	}

	d.activeMode = MessageNew
	d.chat.messageList.SetSelectable(false, false)
	d.chat.ShowMessageBox()
	d.Update()
	d.app.SetFocus(d.chat.messageForm)
}
