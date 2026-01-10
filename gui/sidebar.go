package gui

import (
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/rivo/tview"
)

type Sidebar struct {
	view *tview.Grid

	topicsTable           *tview.Table
	topicForm             *tview.Form
	topicFormInput        *tview.InputField
	topicFormInputContent string

	d *Display
}

type SidebarData struct {
	topics []*razpravljalnica.Topic
}

const noTopicsPlaceholder = `[white]No topics created
[yellow]N[white]: New topic`

const selectTopicPlaceholder = `[white]Select topic
[yellow]N[white]: New topic`

func NewSidebar() *Sidebar {
	table := StyleNormalTable(tview.NewTable())

	grid := tview.NewGrid().SetRows(3, 0, 5).SetColumns(0).
		AddItem(table, 1, 0, 1, 1, 0, 0, true)

	textInput := tview.NewInputField().
		SetLabel("New topic").
		SetFieldWidth(0).
		SetAcceptanceFunc(tview.InputFieldMaxLength(100))

	sidebar := &Sidebar{
		view:           grid,
		topicsTable:    table,
		topicFormInput: textInput,
	}

	table.Select(0, 0).
		SetSelectable(true, false).
		SetSelectedFunc(func(row, column int) {
			topic := table.GetCell(row, column).GetReference().(*razpravljalnica.Topic)
			sidebar.d.ToMessageList(topic)
		})

	textInput.
		SetChangedFunc(func(text string) { sidebar.topicFormInputContent = text })

	topicForm := tview.NewForm().
		AddFormItem(textInput).
		AddButton("Create", func() {
			sidebar.d.createTopic(sidebar.topicFormInputContent)
			sidebar.d.ToTopicMenu()
		}).SetButtonsAlign(tview.AlignRight)

	sidebar.topicForm = topicForm

	return sidebar
}

func (s *Sidebar) GetView() tview.Primitive {
	return s.view
}

func (s *Sidebar) Update(data SidebarData) {
	if len(data.topics) == 0 {
		s.view.AddItem(tview.NewTextView().SetDynamicColors(true).SetText(noTopicsPlaceholder), 0, 0, 1, 1, 0, 0, false)
	} else {
		s.view.AddItem(tview.NewTextView().SetDynamicColors(true).SetText(selectTopicPlaceholder), 0, 0, 1, 1, 0, 0, false)
	}

	s.topicsTable.Clear()

	for i, topic := range data.topics {
		cell := StyleNormalTableCell(tview.NewTableCell(topic.GetName())).
			SetReference(topic).
			SetExpansion(1)

		if s.d.selectedTopic != nil && s.d.selectedTopic.Id == topic.Id {
			StyleSelectedTableCell(cell)
		}

		s.topicsTable.SetCell(i, 0, cell)
	}

}

func (s *Sidebar) ResetNewTopicBox() {
	s.topicFormInput.SetText("")
	s.topicFormInputContent = ""
}

func (s *Sidebar) HideAndResetNewTopicBox() {
	s.view.AddItem(tview.NewBox(), 2, 0, 1, 1, 0, 0, false)
	s.ResetNewTopicBox()
}

func (s *Sidebar) ShowNewTopicBox() {
	s.view.AddItem(s.topicForm, 2, 0, 1, 1, 0, 0, true)
}

func (s *Sidebar) SetSelectable(value bool) {
	current, _ := s.topicsTable.GetSelectable()
	if current != value {
		s.topicsTable.SetSelectable(value, false)
	}
}
