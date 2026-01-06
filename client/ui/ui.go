package ui

import "github.com/rivo/tview"

type AppPage string

const (
	LoginPage AppPage = "login"
	ChatPage  AppPage = "chat"
)

func NewUI() *tview.Application {

	app := tview.NewApplication()
	pages := tview.NewPages()
	app.SetRoot(pages, true).SetFocus(pages)

	header := NewHeader()
	chat := NewChat()

	d := NewDisplay(app, header, chat, pages, UsernameSelection)
	d.RegisterKeyboardHandlers()

	chat.d = d

	loginScreen := newLoginScreen(d, header)
	pages.AddPage(string(LoginPage), loginScreen, true, true)

	chatScreen := newChatScreen(d, header, chat)
	pages.AddPage(string(ChatPage), chatScreen, true, false)

	return app
}
