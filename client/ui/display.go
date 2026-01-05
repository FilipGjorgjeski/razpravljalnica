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
	ChatType
	ChatScroll
	ChatEdit
)

type Display struct {
	username      string
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
			case ChatScroll:
				d.ToTopicMenu()
			}
		}
		return event
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
		username:      d.username,
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

func (d *Display) SelectTopic(topic *razpravljalnica.Topic) {
	d.selectedTopic = topic
	d.activeMode = ChatScroll
	d.Update()
	d.app.SetFocus(d.chat.GetView())
}

func (d *Display) GetUsername() string {
	return d.username
}

func (d *Display) SetUsername(username string) {
	d.username = username
	d.Update()
}

func (d *Display) ToTopicMenu() {
	d.activeMode = TopicMenu
	d.selectedTopic = &razpravljalnica.Topic{}
	d.Update()
}
