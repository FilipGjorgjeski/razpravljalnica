package ui

import (
	"github.com/FilipGjorgjeski/razpravljalnica/client/connection"
	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/rivo/tview"
)

func newSidebar(d *Display) tview.Primitive {

	table := tview.NewTable()

	for i, topic := range connection.Topics {
		addSidebarTopicEntry(table, topic, i)
	}

	return table.Select(0, 0).SetSelectable(true, false).SetSelectedFunc(func(row, column int) {
		topic := table.GetCell(row, column).GetReference().(*razpravljalnica.Topic)
		d.SelectTopic(topic)
	})
}

func addSidebarTopicEntry(table *tview.Table, topic *razpravljalnica.Topic, row int) {
	cell := tview.NewTableCell(topic.GetName()).SetReference(topic)
	table.SetCell(row, 0, cell)
}
