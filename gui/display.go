package gui

import (
	"context"
	"slices"
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
	TopicNew          DisplayMode = "TOPIC_NEW"
	MessageNew        DisplayMode = "MESSAGE_NEW"
	MessageList       DisplayMode = "MESSAGE_LIST"
	MessageEdit       DisplayMode = "MESSAGE_EDIT"
)

type Display struct {
	user       *razpravljalnica.User
	activeMode DisplayMode

	topicsList   []*razpravljalnica.Topic
	messagesList []*razpravljalnica.Message

	selectedTopic   *razpravljalnica.Topic
	selectedMessage *razpravljalnica.Message

	activeSubscriptionListeners []int64

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
		if event.Key() == tcell.KeyDelete {
			if d.activeMode == MessageList {
				message := d.chat.GetHighlightedMessage()
				if message.UserId != d.user.Id {
					return event
				}

				go d.deleteMessage(message.Id)
			}
		}
		if event.Rune() == 'n' {
			switch d.activeMode {
			case TopicMenu:
				d.ToNewTopicBox()
			case MessageList:
				d.ToMessageBox(nil)
			}
		}
		if event.Rune() == 'l' {
			if d.activeMode == MessageList {
				message := d.chat.GetHighlightedMessage()

				go d.likeMessage(message.Id)
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
			d.sidebar.SetSelectable(false)
			d.sidebar.HideAndResetNewTopicBox()
			d.chat.SetSelectable(false)
			d.chat.HideAndResetMessageBox()
		case TopicMenu:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.sidebar.GetView())
			d.sidebar.SetSelectable(true)
			d.sidebar.HideAndResetNewTopicBox()
			d.chat.SetSelectable(false)
			d.chat.HideAndResetMessageBox()
		case TopicNew:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.sidebar.topicForm)
			d.sidebar.SetSelectable(false)
			d.sidebar.ShowNewTopicBox()
			d.chat.SetSelectable(false)
			d.chat.HideAndResetMessageBox()
		case MessageList:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.GetView())
			d.sidebar.SetSelectable(false)
			d.sidebar.HideAndResetNewTopicBox()
			d.chat.SetSelectable(true)
			d.chat.HideAndResetMessageBox()
		case MessageNew:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.messageForm)
			d.sidebar.SetSelectable(false)
			d.sidebar.HideAndResetNewTopicBox()
			d.chat.SetSelectable(false)
			d.chat.ShowMessageBox()
			d.chat.messageFormInput.SetLabel("New message")
		case MessageEdit:
			d.pages.SwitchToPage(string(ChatPage))
			d.app.SetFocus(d.chat.messageForm)
			d.sidebar.SetSelectable(false)
			d.sidebar.HideAndResetNewTopicBox()
			d.chat.SetSelectable(false)
			d.chat.ShowMessageBox()
			d.chat.messageFormInput.SetLabel("Edit message")
		}

		d.header.Update(HeaderData{
			username:           d.user.GetName(),
			clusterState:       d.client.State().String(),
			err:                "",
			displayMode:        d.activeMode,
			highlightedMessage: d.chat.GetHighlightedMessage(),
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
		username:           d.GetUsername(),
		clusterState:       d.client.State().String(),
		err:                err.Error(),
		displayMode:        d.activeMode,
		highlightedMessage: d.chat.GetHighlightedMessage(),
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

func (d *Display) ToNewTopicBox() {
	d.lock.Lock()

	d.activeMode = TopicNew
	d.selectedTopic = nil
	d.selectedMessage = nil

	d.sidebar.topicForm.SetFocus(0)

	d.lock.Unlock()
	d.Update()
}

func (d *Display) fetchAndUpdateMessageList(topicId int64) {
	ctx, cancel := TimeoutContext()
	defer cancel()
	messages, err := d.client.GetMessages(ctx, topicId, 0, 0, d.user.Id)
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
		go d.createSubscriptionListener(topic.Id)
	}
}

// Focus message box, editing message if provided
func (d *Display) ToMessageBox(selectedMessage *razpravljalnica.Message) {
	d.lock.Lock()

	if selectedMessage != nil && selectedMessage.UserId != d.user.Id {
		// Attempting to edit other person's message
		d.lock.Unlock()
		return
	}

	if selectedMessage == nil {
		d.activeMode = MessageNew
		d.selectedMessage = nil
	} else {
		d.activeMode = MessageEdit
		d.selectedMessage = selectedMessage
		d.chat.messageFormInput.SetText(selectedMessage.Text)
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

func (d *Display) createTopic(name string) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	_, err := d.client.CreateTopic(ctx, name, "")
	if err != nil {
		d.handleError(err)
	}

	d.Update()
}

func (d *Display) deleteMessage(messageId int64) {
	ctx, cancel := TimeoutContext()
	defer cancel()

	err := d.client.DeleteMessage(ctx, d.selectedTopic.Id, d.user.Id, messageId, "")
	if err != nil {
		d.handleError(err)
	}

	d.Update()
}

func (d *Display) getMessageListIndex(id int64) int {
	return slices.IndexFunc(d.messagesList, func(message *razpravljalnica.Message) bool {
		return message.Id == id
	})
}

func (d *Display) createSubscriptionListener(topicId int64) {
	d.lock.Lock()
	exists := slices.Contains(d.activeSubscriptionListeners, topicId)
	d.lock.Unlock()
	if exists {
		return
	}

	ctx := context.Background()
	err := d.client.Subscribe(ctx, []int64{topicId}, d.user.Id, 0, func(me *razpravljalnica.MessageEvent) error {
		d.lock.Lock()
		defer d.lock.Unlock()
		// Chat window is not focused on this topic
		if d.selectedTopic == nil || d.selectedTopic.GetId() != me.Message.GetTopicId() {
			return nil
		}

		index := d.getMessageListIndex(me.Message.GetId())

		switch me.Op {
		case razpravljalnica.OpType_OP_POST:
			if index == -1 {
				d.messagesList = append(d.messagesList, me.Message)
			}
		case razpravljalnica.OpType_OP_UPDATE, razpravljalnica.OpType_OP_LIKE:
			d.messagesList[index] = me.Message
		case razpravljalnica.OpType_OP_DELETE:
			d.messagesList = slices.Delete(d.messagesList, index, index+1)
		}

		d.Update()
		return nil
	})

	if err != nil {
		d.handleError(err)
	}

}
