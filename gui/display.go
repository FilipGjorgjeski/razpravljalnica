package gui

import (
	"sync"

	"github.com/FilipGjorgjeski/razpravljalnica/client"
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type DisplayMode string

const (
	UsernameSelection DisplayMode = "USERNAME_SELECTION"
	TopicMenu         DisplayMode = "TOPIC_MENU"
	MessageNew        DisplayMode = "MESSAGE_NEW"
	MessageList       DisplayMode = "MESSAGE_LIST"
	MessageEdit       DisplayMode = "MESSAGE_EDIT"
)

const colorFieldSelected = tcell.ColorDarkGray

type Display struct {
	user       *razpravljalnica.User
	activeMode DisplayMode

	topicsList   []*razpravljalnica.Topic
	messagesList []*razpravljalnica.Message

	selectedTopic   *razpravljalnica.Topic
	selectedMessage *razpravljalnica.Message

	lock sync.RWMutex

	chat    *Chat
	header  *Header
	sidebar *Sidebar
	pages   *tview.Pages
	app     *tview.Application
	client  *client.Client
}

func NewDisplay(app *tview.Application, header *Header, chat *Chat, sidebar *Sidebar, pages *tview.Pages, defaultMode DisplayMode, cl *client.Client) *Display {
	return &Display{
		activeMode:    defaultMode,
		selectedTopic: &razpravljalnica.Topic{},

		pages:   pages,
		header:  header,
		sidebar: sidebar,
		chat:    chat,
		app:     app,
		client:  cl,
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
			case MessageNew, MessageEdit:
				d.ToMessageList(nil)
			}
		}
		if event.Key() == tcell.KeyCtrlR {
			d.Update()
		}
		if event.Rune() == 'n' {
			switch d.activeMode {
			case TopicMenu:
				// TODO: Create topic
			case MessageList:
				d.ToMessageBox(nil)
			}
		}
		if event.Rune() == 'l' {
			if d.activeMode == MessageList {
				highlightedMessage := d.chat.messageList.GetCell(d.chat.highlightedRow, 2).Reference.(*razpravljalnica.Message)

				go d.likeMessage(highlightedMessage.Id)
			}
		}
		return event
	})
}

// Updates display with current data. Locks Display when run.
func (d *Display) Update() {
	go d.app.QueueUpdateDraw(func() {
		d.lock.Lock()
		defer d.lock.Unlock()
		// Handle page state and focus
		switch d.activeMode {
		case UsernameSelection:
			d.pages.SwitchToPage(string(LoginPage))
			d.app.SetFocus(d.chat.GetView())
			d.chat.SetSelectable(false)
			d.sidebar.topicsTable.SetSelectable(false, false)
			d.chat.HideMessageBox()
			d.chat.ResetMessageBox()
		case TopicMenu:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.sidebar.GetView())
			d.chat.SetSelectable(false)
			d.sidebar.topicsTable.SetSelectable(true, false)
			d.chat.HideMessageBox()
			d.chat.ResetMessageBox()
		case MessageList:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.GetView())
			d.chat.SetSelectable(true)
			d.sidebar.topicsTable.SetSelectable(false, false)
			d.chat.HideMessageBox()
			d.chat.ResetMessageBox()
		case MessageNew:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.messageForm)
			d.chat.SetSelectable(false)
			d.sidebar.topicsTable.SetSelectable(false, false)
			d.chat.ShowMessageBox()
			d.chat.messageFormInput.SetLabel("New message")
		case MessageEdit:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.messageForm)
			d.chat.SetSelectable(false)
			d.sidebar.topicsTable.SetSelectable(false, false)
			d.chat.ShowMessageBox()
			d.chat.messageFormInput.SetLabel("Edit message")
		}

		d.header.Update(HeaderData{
			username:     d.user.GetName(),
			clusterState: d.client.State().String(),
			err:          "",
			displayMode:  d.activeMode,
		})

		d.chat.Update(ChatData{
			messages: d.messagesList,
		})

		d.sidebar.Update(SidebarData{
			topics: d.topicsList,
		})
	})

}

func (d *Display) GetSelectedTopic() *razpravljalnica.Topic {
	return d.selectedTopic
}

func (d *Display) GetUsername() string {
	return d.user.GetName()
}

func (d *Display) Login(username string) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	user, err := d.client.CreateUser(ctx, username, "")
	if err != nil {
		d.handleError(err)
		return
	}

	d.lock.Lock()
	d.user = user
	d.lock.Unlock()

	d.Update()
}

func (d *Display) handleError(err error) {
	d.header.Update(HeaderData{
		username:     d.GetUsername(),
		clusterState: d.client.State().String(),
		err:          err.Error(),
		displayMode:  d.activeMode,
	})
}

func (d *Display) fetchAndUpdateTopicsList() {
	ctx, cancel := TimeoutContext()
	defer cancel()
	topics, err := d.client.ListTopics(ctx)
	if err != nil {
		d.handleError(err)
		return
	}

	d.lock.Lock()
	d.topicsList = topics.GetTopics()
	d.lock.Unlock()

	d.Update()
}

// Focus topic selection list
func (d *Display) ToTopicMenu() {

	d.lock.Lock()

	d.activeMode = TopicMenu
	d.selectedTopic = nil
	d.selectedMessage = nil
	d.messagesList = []*razpravljalnica.Message{}

	d.lock.Unlock()

	d.Update()

	go d.fetchAndUpdateTopicsList()
}

func (d *Display) fetchAndUpdateMessageList(topicId int64) {
	ctx, cancel := TimeoutContext()
	defer cancel()
	messages, err := d.client.GetMessages(ctx, topicId, 0, 0)
	if err != nil {
		d.handleError(err)
		return
	}

	d.lock.Lock()
	d.messagesList = messages.GetMessages()
	d.lock.Unlock()

	d.Update()
}

// Focus message list of specific topic, if topic is nil, current active topic will not be changed (assumed returning from message editing)
func (d *Display) ToMessageList(topic *razpravljalnica.Topic) {
	d.lock.Lock()
	d.selectedMessage = nil
	d.activeMode = MessageList

	if topic != nil {
		d.selectedTopic = topic
	}
	d.lock.Unlock()

	d.Update()

	if topic != nil {
		go d.fetchAndUpdateMessageList(topic.Id)
	}
}

// Focus message box, editing message if provided
func (d *Display) ToMessageBox(selectedMessage *razpravljalnica.Message) {
	d.lock.Lock()
	if selectedMessage == nil || selectedMessage.UserId != d.user.Id {
		d.activeMode = MessageNew
		d.selectedMessage = nil
	} else {
		d.activeMode = MessageEdit
		d.selectedMessage = selectedMessage
	}
	d.chat.messageForm.SetFocus(0)

	d.lock.Unlock()

	d.Update()
}

func (d *Display) sendMessage(topicId, userId int64, text string) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	_, err := d.client.PostMessage(ctx, topicId, userId, text, "")
	if err != nil {
		d.handleError(err)
	}
}

func (d *Display) updateMessage(topicId, userId, messageId int64, text string) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	_, err := d.client.UpdateMessage(ctx, topicId, userId, messageId, text, "")
	if err != nil {
		d.handleError(err)
	}
}

func (d *Display) sendOrUpdateMessage(text string) {
	if d.selectedMessage == nil {
		go d.sendMessage(d.selectedTopic.Id, d.user.Id, text)
	} else {
		go d.updateMessage(d.selectedTopic.Id, d.user.Id, d.selectedMessage.Id, text)
	}

}

func (d *Display) likeMessage(messageId int64) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	_, err := d.client.LikeMessage(ctx, d.selectedTopic.Id, messageId, d.user.Id, "")
	if err != nil {
		d.handleError(err)
	}

	d.Update()
}
