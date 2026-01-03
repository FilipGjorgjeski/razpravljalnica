package ui

import "github.com/rivo/tview"

var pages *tview.Pages

func SwitchUIPage(name string) {
	pages.SwitchToPage(name)
}

func NewUI() *tview.Application {
	//root := tview.NewBox().SetBorder(true).SetTitle("razpravljalnica")
	loginScreen := newLoginScreen()
	chatScreen := newChatScreen()

	pages = tview.NewPages()
	pages.AddPage("login", loginScreen, true, true)
	pages.AddPage("chat", chatScreen, true, false)

	app := tview.NewApplication().SetRoot(pages, true).SetFocus(pages)

	return app
}
