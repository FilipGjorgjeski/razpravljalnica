package gui

import (
	"github.com/FilipGjorgjeski/razpravljalnica/client"
	"github.com/rivo/tview"
)

type AppPage string

const (
	LoginPage AppPage = "login"
	ChatPage  AppPage = "chat"
)

func NewApp(cl *client.Client) *tview.Application {

	app := tview.NewApplication()
	pages := tview.NewPages()
	app.SetRoot(pages, true).SetFocus(pages)

	header := NewHeader()
	chat := NewChat()
	sidebar := NewSidebar()

	d := NewDisplay(app, header, chat, sidebar, pages, UsernameSelection, cl)
	d.RegisterKeyboardHandlers()

	chat.d = d
	sidebar.d = d

	loginScreen := newLoginScreen(d, header)
	pages.AddPage(string(LoginPage), loginScreen, true, true)

	chatScreen := newChatScreen(header, chat, sidebar)
	pages.AddPage(string(ChatPage), chatScreen, true, false)

	return app
}
