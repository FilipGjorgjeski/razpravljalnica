package gui

import (
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/rivo/tview"
)

type Sidebar struct {
	view *tview.Grid

	topicsTable *tview.Table

	d *Display
}

type SidebarData struct {
	topics []*razpravljalnica.Topic
}

func NewSidebar() *Sidebar {
	table := tview.NewTable()

	grid := tview.NewGrid().SetRows(0).SetColumns(0).
		AddItem(table, 0, 0, 1, 1, 0, 0, true)

	sidebar := &Sidebar{
		view:        grid,
		topicsTable: table,
	}

	table.Select(0, 0).SetSelectable(true, false).SetSelectedFunc(func(row, column int) {
		topic := table.GetCell(row, column).GetReference().(*razpravljalnica.Topic)
		sidebar.d.ToMessageList(topic)
	})

	return sidebar
}

func (s *Sidebar) GetView() tview.Primitive {
	return s.view
}

func (s *Sidebar) Update(data SidebarData) {
	s.topicsTable.Clear()

	for i, topic := range data.topics {
		cell := tview.NewTableCell(topic.GetName()).SetReference(topic)
		if s.d.selectedTopic != nil && s.d.selectedTopic.Id == topic.Id {
			cell.SetBackgroundColor(colorFieldSelected)
		}
		s.topicsTable.SetCell(i, 0, cell)
	}
}
