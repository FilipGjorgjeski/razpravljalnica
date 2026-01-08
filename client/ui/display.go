package ui

import (
	"sync"

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
	user            *razpravljalnica.User
	activeMode      DisplayMode
	selectedTopic   *razpravljalnica.Topic
	selectedMessage *razpravljalnica.Message

	lock sync.RWMutex

	chat   *Chat
	header *Header
	pages  *tview.Pages
	app    *tview.Application
	conn   *connection.Connection
}

func NewDisplay(app *tview.Application, header *Header, chat *Chat, pages *tview.Pages, defaultMode DisplayMode, conn *connection.Connection) *Display {
	return &Display{
		activeMode:    defaultMode,
		selectedTopic: &razpravljalnica.Topic{},

		pages:  pages,
		header: header,
		chat:   chat,
		app:    app,
		conn:   conn,
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
		if event.Key() == tcell.KeyCtrlR {
			d.Update()
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
	d.lock.Lock()
	defer d.lock.Unlock()
	// Handle page
	if d.activeMode == UsernameSelection {
		d.pages.SwitchToPage(string(LoginPage))
	} else {
		d.pages.SwitchToPage(string(ChatPage))
	}

	d.header.Update(HeaderData{
		username:         d.user.GetName(),
		connectionStatus: d.conn.Status(),
	})

	messages := d.conn.GetMessages()

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
	d.conn.FetchTopics()
	d.selectedMessage = nil
	d.activeMode = TopicMenu
	d.selectedTopic = &razpravljalnica.Topic{}
	d.chat.messageList.SetSelectable(false, false)
	d.Update()
}

// Focus message list of specific topic, if topic is nil, current active topic will not be changed
func (d *Display) ToMessageList(topic *razpravljalnica.Topic) {
	d.selectedMessage = nil
	if topic != nil {
		d.selectedTopic = topic
		d.conn.FetchTopicMessages(topic.Id)
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
		d.selectedMessage = nil
	} else {
		d.chat.messageFormInput.SetLabel("Edit message")
		d.selectedMessage = selectedMessage
	}

	d.activeMode = MessageNew
	d.chat.messageList.SetSelectable(false, false)
	d.chat.ShowMessageBox()
	d.Update()
	d.app.SetFocus(d.chat.messageForm)
}
